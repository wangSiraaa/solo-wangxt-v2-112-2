package backup

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"incbackup/internal/repo"
)

// Selective restore jobs move through the same four states as their SQLite
// rows: pending -> running -> completed, or either active state -> failed.
const (
	RestoreStagePreflight = "preflight"
	RestoreStagePrepare   = "prepare"
	RestoreStageEntries   = "entries"
	RestoreStageMetadata  = "metadata"
	RestoreStagePublish   = "publish"
	RestoreStageRecover   = "recover"
)

// SelectiveRestoreRequest freezes one committed snapshot id and a set of
// normalized relative paths. Paths are relative to the snapshot root;
// selecting a directory selects its recorded subtree.
type SelectiveRestoreRequest struct {
	SnapshotID     int64
	Target         string
	Paths          []string
	IdempotencyKey string
}

// SelectiveRestoreError reports a rejected request before any destination or
// private staging directory was created.
type SelectiveRestoreError struct {
	Status  int
	Code    string
	Reasons []string
}

func (e *SelectiveRestoreError) Error() string {
	return e.Code + ": " + strings.Join(e.Reasons, "; ")
}

func newSelectiveError(status int, code string, reasons ...string) error {
	return &SelectiveRestoreError{Status: status, Code: code, Reasons: reasons}
}

type selectivePlan struct {
	target    string
	requested []string
	entries   []repo.StoredEntry
	root      *repo.StoredEntry
}

// CreateSelectiveRestore validates and persists a selective restore job, then
// starts it in the background. Repeated requests carrying the same
// idempotency key return the original job even if it has failed or completed.
func (e *Engine) CreateSelectiveRestore(req SelectiveRestoreRequest) (*repo.RestoreJobInfo, bool, error) {
	e.restoreMu.Lock()
	defer e.restoreMu.Unlock()

	if req.IdempotencyKey != "" {
		if existing, ok, err := e.Manifest.GetRestoreJobByIdempotencyKey(req.IdempotencyKey); err != nil {
			return nil, false, err
		} else if ok {
			if err := validateIdempotentRequest(e.Manifest, req, existing); err != nil {
				return nil, false, err
			}
			return &existing, false, nil
		}
	}

	plan, err := e.prepareSelectivePlan(req, true)
	if err != nil {
		return nil, false, err
	}
	suffix, err := randomSuffix()
	if err != nil {
		return nil, false, err
	}
	job, err := e.Manifest.CreateRestoreJob(repo.RestoreJobCreate{
		SnapshotID:  req.SnapshotID,
		Target:      plan.target,
		Idempotency: req.IdempotencyKey,
		Paths:       plan.requested,
		Entries:     jobEntryReports(plan.entries),
		StagingName: suffix,
	})
	if err != nil {
		return nil, false, err
	}
	e.startRestoreJob(job.ID)
	return &job, true, nil
}

func validateIdempotentRequest(m *repo.Manifest, req SelectiveRestoreRequest, existing repo.RestoreJobInfo) error {
	target, err := filepath.Abs(filepath.FromSlash(strings.TrimSpace(req.Target)))
	if err != nil {
		return err
	}
	paths, err := normalizeRelativePaths(req.Paths)
	if err != nil {
		return newSelectiveError(400, "bad_path", err.Error())
	}
	frozen, err := m.RestoreJobPaths(existing.ID, repo.RestorePathRequested)
	if err != nil {
		return err
	}
	if req.SnapshotID != existing.SnapshotID || target != existing.Target ||
		!equalStringSet(paths, frozen) {
		return newSelectiveError(409, "idempotency_key_conflict",
			"idempotency key already belongs to a selective restore with a different snapshot, target, or normalized path set")
	}
	return nil
}

func equalStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		if seen[s] == 0 {
			return false
		}
		seen[s]--
	}
	return true
}

// RetrySelectiveRestore queues a failed selective restore for another attempt.
// It re-runs preflight against the immutable snapshot and current filesystem.
func (e *Engine) RetrySelectiveRestore(id int64) (*repo.RestoreJobInfo, error) {
	e.restoreMu.Lock()
	defer e.restoreMu.Unlock()

	job, err := e.Manifest.GetRestoreJob(id)
	if err != nil {
		return nil, err
	}
	if job.Status != repo.RestoreStatusFailed {
		return nil, fmt.Errorf("restore job %d is %s; only failed jobs can be retried", id, job.Status)
	}
	paths, err := e.Manifest.RestoreJobPaths(id, repo.RestorePathRequested)
	if err != nil {
		return nil, err
	}
	plan, err := e.prepareSelectivePlan(SelectiveRestoreRequest{
		SnapshotID: job.SnapshotID, Target: job.Target, Paths: paths,
	}, true)
	if err != nil {
		return nil, err
	}
	suffix, err := randomSuffix()
	if err != nil {
		return nil, err
	}
	staging := fmt.Sprintf(".increstore-job-%d-%s", id, suffix)
	if err := e.Manifest.ResetFailedRestoreJob(id, staging, jobEntryReports(plan.entries)); err != nil {
		return nil, err
	}
	e.startRestoreJob(id)
	job, err = e.Manifest.GetRestoreJob(id)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// GetSelectiveRestore returns one durable job.
func (e *Engine) GetSelectiveRestore(id int64) (*repo.RestoreJobInfo, error) {
	j, err := e.Manifest.GetRestoreJob(id)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// SelectiveRestoreFileReports returns the per-entry status and measurements.
func (e *Engine) SelectiveRestoreFileReports(id int64) ([]repo.RestoreJobEntry, error) {
	return e.Manifest.ListRestoreJobEntries(id)
}

func (e *Engine) prepareSelectivePlan(req SelectiveRestoreRequest, checkTarget bool) (selectivePlan, error) {
	var plan selectivePlan
	info, err := e.Manifest.GetSnapshot(req.SnapshotID)
	if errors.Is(err, repo.ErrNotFound) {
		return plan, newSelectiveError(404, "snapshot_not_found",
			fmt.Sprintf("snapshot %d does not exist", req.SnapshotID))
	}
	if err != nil {
		return plan, err
	}
	if info.Status != repo.StatusCommitted {
		return plan, newSelectiveError(409, "snapshot_not_committed",
			fmt.Sprintf("snapshot %d is %s", req.SnapshotID, info.Status))
	}

	target, err := filepath.Abs(filepath.FromSlash(strings.TrimSpace(req.Target)))
	if err != nil {
		return plan, err
	}
	if strings.TrimSpace(req.Target) == "" || filepath.Dir(target) == target || target == string(filepath.Separator) {
		return plan, newSelectiveError(400, "bad_target", "target must be a non-root, absolute or cwd-relative directory path")
	}
	if checkTarget {
		if err := noSymlinkAncestors(target); err != nil {
			return plan, newSelectiveError(422, "unsafe_target", err.Error())
		}
		if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return plan, fmt.Errorf("%w: %s", ErrTargetExists, target)
			}
			return plan, err
		}
	}

	requested, err := normalizeRelativePaths(req.Paths)
	if err != nil {
		return plan, newSelectiveError(400, "bad_path", err.Error())
	}
	if len(requested) == 0 {
		return plan, newSelectiveError(400, "bad_request", "at least one relative path is required")
	}

	entries, err := e.Manifest.EntriesOf(req.SnapshotID)
	if err != nil {
		return plan, err
	}
	if err := validateEntryPaths(entries); err != nil {
		return plan, err
	}
	byPath := make(map[string]*repo.StoredEntry, len(entries))
	for i := range entries {
		byPath[entries[i].RelPath] = &entries[i]
	}
	root := byPath["."]
	if root == nil {
		return plan, fmt.Errorf("snapshot %d has no root directory entry", req.SnapshotID)
	}

	var reasons []string
	for _, p := range requested {
		if _, ok := byPath[p]; !ok {
			reasons = append(reasons, "path is not present in snapshot: "+p)
		}
	}
	if len(reasons) > 0 {
		return plan, newSelectiveError(404, "paths_not_found", reasons...)
	}

	effective := map[string]bool{}
	for _, p := range requested {
		addSelection(byPath, effective, p)
	}
	for _, p := range requested {
		for _, parent := range ancestorPaths(p) {
			if _, ok := byPath[parent]; ok {
				effective[parent] = true
			}
		}
	}

	// Every selected internal symlink must have the links in its resolution
	// chain and the final in-snapshot target explicitly included. We never
	// rewrite the stored target string to paper over a missing dependency.
	for _, se := range entries {
		if se.Kind != repo.KindSymlink || !effective[se.RelPath] {
			continue
		}
		if err := validateLinkTarget(target, se.RelPath, se.LinkTarget); err != nil {
			reasons = append(reasons, err.Error())
			continue
		}
		required, rerr := requiredLinkTargets(byPath, target, se.RelPath, se.LinkTarget)
		if rerr != nil {
			reasons = append(reasons, rerr.Error())
			continue
		}
		for _, dep := range required {
			if !effective[dep] {
				reasons = append(reasons, fmt.Sprintf(
					"symlink %q depends on unselected internal path %q; add it to paths", se.RelPath, dep))
			}
		}
	}
	if len(reasons) > 0 {
		sort.Strings(reasons)
		reasons = dedupeStrings(reasons)
		return plan, newSelectiveError(422, "missing_symlink_dependency", reasons...)
	}

	var chosen []repo.StoredEntry
	for _, se := range entries {
		if effective[se.RelPath] {
			chosen = append(chosen, se)
		}
	}
	// Preflight chunk liveness checks. Stream verification remains the
	// authoritative check for corruption during execution.
	for _, se := range chosen {
		if se.Kind != repo.KindFile {
			continue
		}
		if int64(len(se.ChunkDigests)) == 0 && se.Size != 0 {
			reasons = append(reasons, fmt.Sprintf("%s has size %d but no chunk records", se.RelPath, se.Size))
			continue
		}
		for _, d := range se.ChunkDigests {
			length, hasRow, err := e.Manifest.ChunkLength(d)
			if err != nil {
				return plan, err
			}
			if !hasRow {
				reasons = append(reasons, fmt.Sprintf("%s: chunk %s missing from catalog",
					se.RelPath, hex.EncodeToString(d)))
				continue
			}
			ok, err := e.Store.Has(d, length)
			if err != nil {
				return plan, err
			}
			if !ok {
				reasons = append(reasons, fmt.Sprintf("%s: chunk %s missing or has wrong length in content store",
					se.RelPath, hex.EncodeToString(d)))
			}
		}
	}
	if len(reasons) > 0 {
		return plan, newSelectiveError(422, "missing_content", reasons...)
	}

	plan.target = target
	plan.requested = requested
	plan.entries = chosen
	plan.root = root
	return plan, nil
}

func normalizeRelativePaths(in []string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, raw := range in {
		s := filepath.ToSlash(strings.TrimSpace(raw))
		if s == "" {
			continue
		}
		if filepath.IsAbs(filepath.FromSlash(s)) {
			return nil, fmt.Errorf("absolute path is not allowed: %q", raw)
		}
		clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(s)))
		if clean == ".." || strings.HasPrefix(clean, "../") {
			return nil, fmt.Errorf("path escapes snapshot root: %q", raw)
		}
		if clean == "." || clean == "/" {
			clean = "."
		}
		if !seen[clean] {
			seen[clean] = true
			out = append(out, clean)
		}
	}
	sort.Strings(out)
	return out, nil
}

func addSelection(byPath map[string]*repo.StoredEntry, set map[string]bool, rel string) {
	set[rel] = true
	if byPath[rel] != nil && byPath[rel].Kind == repo.KindDir {
		prefix := rel + "/"
		for p := range byPath {
			if rel == "." || strings.HasPrefix(p, prefix) {
				set[p] = true
			}
		}
	}
}

func ancestorPaths(rel string) []string {
	if rel == "." || rel == "" {
		return nil
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	var out []string
	cur := ""
	for _, part := range parts[:len(parts)-1] {
		if cur == "" {
			cur = part
		} else {
			cur += "/" + part
		}
		out = append(out, cur)
	}
	return out
}

// requiredLinkTargets lexically follows manifest symlinks without touching the
// filesystem. It includes intermediate symlink entries and the final target.
func requiredLinkTargets(byPath map[string]*repo.StoredEntry, target, rel, linkTo string) ([]string, error) {
	targetEntry, links, err := resolveManifestPath(byPath, target, rel, linkTo)
	if err != nil {
		return nil, err
	}
	if targetEntry == nil {
		return nil, fmt.Errorf("symlink %q -> %q points to a path not present in the snapshot", rel, linkTo)
	}
	resolved := targetEntry.RelPath
	if byPath[resolved] == nil {
		return nil, fmt.Errorf("symlink %q -> %q points to %q, which is not present in the snapshot",
			rel, linkTo, resolved)
	}
	for _, l := range links {
		if l != rel {
			se := byPath[l]
			if se == nil {
				return nil, fmt.Errorf("symlink %q depends on unrecorded link %q", rel, l)
			}
		}
	}
	out := append([]string{}, links...)
	if resolved != rel {
		out = append(out, resolved)
	}
	return dedupeStrings(out), nil
}

// resolveManifestPath follows in-manifest symlink prefixes. root is the lexical
// restore target; raw is either absolute ("/..." from target root for relative
// links) or a stored absolute symlink target.
func resolveManifestPath(byPath map[string]*repo.StoredEntry, target, linkRel, raw string) (*repo.StoredEntry, []string, error) {
	var current string
	p := filepath.FromSlash(raw)
	if filepath.IsAbs(p) {
		current = filepath.Clean(p)
	} else {
		raw = filepath.ToSlash(filepath.Clean(filepath.Join("/",
			filepath.Dir(filepath.FromSlash(linkRel)), p)))
		current = filepath.Clean(target + filepath.FromSlash(raw))
	}
	var links []string
	visited := map[string]bool{}
	for {
		rel, inside := lexicalRel(target, current)
		if !inside {
			return nil, nil, fmt.Errorf("symlink target %q escapes restore root", raw)
		}
		if rel == "." {
			return byPath["."], links, nil
		}
		// Exact hit. If it is another symlink, expand it.
		if se := byPath[rel]; se != nil {
			if se.Kind != repo.KindSymlink {
				return se, links, nil
			}
			if visited[rel] {
				return nil, nil, fmt.Errorf("symlink cycle through %q", rel)
			}
			visited[rel] = true
			links = append(links, rel)
			current = expandManifestLink(target, rel, se.LinkTarget)
			continue
		}
		// Otherwise find the longest existing symlink prefix. A path such as
		// "linkdir/child" expands the stored "linkdir" target and preserves
		// "/child" as the remaining suffix.
		parts := strings.Split(rel, "/")
		prefix := ""
		var linkEntry *repo.StoredEntry
		for _, part := range parts {
			if prefix == "" {
				prefix = part
			} else {
				prefix += "/" + part
			}
			se := byPath[prefix]
			if se == nil {
				continue
			}
			if se.Kind != repo.KindDir && se.Kind != repo.KindSymlink {
				return nil, nil, fmt.Errorf("path %q traverses non-directory %q", rel, prefix)
			}
			if se.Kind == repo.KindSymlink {
				linkEntry = se
				break
			}
		}
		if linkEntry == nil {
			return byPath[rel], links, nil
		}
		if visited[linkEntry.RelPath] {
			return nil, nil, fmt.Errorf("symlink cycle through %q", linkEntry.RelPath)
		}
		visited[linkEntry.RelPath] = true
		links = append(links, linkEntry.RelPath)
		current = expandManifestLink(target, linkEntry.RelPath, linkEntry.LinkTarget)
		if suffix := strings.TrimPrefix(rel, linkEntry.RelPath+"/"); suffix != rel && suffix != "" {
			current = filepath.Clean(filepath.Join(current, filepath.FromSlash(suffix)))
		}
	}
}

func expandManifestLink(target, rel, linkTo string) string {
	p := filepath.FromSlash(linkTo)
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Clean(filepath.Join(target, filepath.FromSlash(rel), "..", p))
}

func lexicalRel(root, p string) (string, bool) {
	root = filepath.Clean(root)
	p = filepath.Clean(p)
	if p == root {
		return ".", true
	}
	if strings.HasPrefix(p, root+string(os.PathSeparator)) {
		return filepath.ToSlash(p[len(root)+1:]), true
	}
	return "", false
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := in[:0]
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func jobEntryReports(entries []repo.StoredEntry) []repo.RestoreJobEntry {
	out := make([]repo.RestoreJobEntry, 0, len(entries))
	for i, se := range entries {
		if se.RelPath == "." {
			continue // root metadata is implicit in the published target
		}
		out = append(out, repo.RestoreJobEntry{
			RelPath:    se.RelPath,
			Kind:       se.Kind,
			EntryOrder: i,
			Status:     repo.RestoreEntryPending,
			Size:       se.Size,
			FileDigest: append([]byte(nil), se.FileDigest...),
			Mode:       se.Mode,
			ModTime:    se.ModTime,
			ChunkCount: len(se.ChunkDigests),
			LinkTarget: se.LinkTarget,
		})
	}
	return out
}

func randomSuffix() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (e *Engine) startRestoreJob(id int64) {
	if e.restoreJobs == nil {
		e.restoreJobs = map[int64]struct{}{}
	}
	if _, ok := e.restoreJobs[id]; ok {
		return
	}
	e.restoreJobs[id] = struct{}{}
	go func() {
		defer func() {
			e.restoreMu.Lock()
			delete(e.restoreJobs, id)
			e.restoreMu.Unlock()
		}()
		_ = e.runSelectiveRestore(id)
	}()
}

// runSelectiveRestore constructs the tree in a private sibling directory, only
// publishing it after every chunk, file digest, length and metadata operation
// has succeeded.
func (e *Engine) runSelectiveRestore(id int64) (retErr error) {
	job, err := e.Manifest.GetRestoreJob(id)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("selective restore panic: %v", r)
			_ = e.Manifest.FailRestoreJob(id, RestoreStageEntries, "", retErr.Error())
			cleanupStagingSibling(job.Target, job.StagingPath)
		}
	}()

	stagingName := job.StagingPath
	suffix, err := randomSuffix()
	if err != nil {
		_ = e.Manifest.FailRestoreJob(id, RestoreStagePrepare, "", err.Error())
		return err
	}
	if stagingName == "" {
		stagingName = fmt.Sprintf(".increstore-job-%d-%s", id, suffix)
	}
	staging := filepath.Join(filepath.Dir(job.Target), filepath.Base(stagingName))
	if !isPrivateSibling(job.Target, staging) {
		err := fmt.Errorf("invalid private staging path %q", staging)
		_ = e.Manifest.FailRestoreJob(id, RestoreStagePrepare, "", err.Error())
		return err
	}
	// A retry after a hard crash owns only the staging basename persisted in
	// its own job row. Never remove the destination or another job's tree.
	cleanupStagingSibling(job.Target, stagingName)
	parent := filepath.Dir(job.Target)
	if err := noSymlinkAncestors(job.Target); err != nil {
		_ = e.Manifest.FailRestoreJob(id, RestoreStagePrepare, "", err.Error())
		return err
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		_ = e.Manifest.FailRestoreJob(id, RestoreStagePrepare, "", err.Error())
		return err
	}
	if err := e.Manifest.StartRestoreJob(id, staging); err != nil {
		return err
	}
	failed := false
	failJob := func(stage, rel string, cause error) error {
		failed = true
		msg := ""
		if cause != nil {
			msg = cause.Error()
		}
		_ = e.Manifest.FailRestoreJob(id, stage, rel, msg)
		if rel != "" {
			_ = e.Manifest.MarkRestoreJobEntryFailed(id, rel, msg)
		}
		cleanupStagingSibling(job.Target, stagingName)
		return cause
	}

	plan, err := e.prepareSelectivePlan(SelectiveRestoreRequest{
		SnapshotID: job.SnapshotID, Target: job.Target,
		Paths: mustJobPaths(e.Manifest, id),
	}, false)
	if err != nil {
		return failJob(RestoreStagePreflight, "", err)
	}

	if err := os.Mkdir(staging, 0o700); err != nil {
		return failJob(RestoreStagePrepare, "", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			cleanupStagingSibling(job.Target, stagingName)
		}
	}()

	var createdDirs []string
	var files, dirs, symlinks int
	var bytes int64
	for i := range plan.entries {
		se := plan.entries[i]
		if se.RelPath == "." {
			continue
		}
		dst, err := safeJoin(staging, se.RelPath)
		if err != nil {
			return failJob(RestoreStageEntries, se.RelPath, err)
		}
		report := repo.RestoreJobEntry{
			RelPath: se.RelPath, Kind: se.Kind, EntryOrder: i,
			Size: se.Size, FileDigest: se.FileDigest, Mode: se.Mode,
			ModTime: se.ModTime, ChunkCount: len(se.ChunkDigests),
			LinkTarget: se.LinkTarget, Status: repo.RestoreEntryRunning,
		}
		_ = e.Manifest.UpdateRestoreJobEntry(id, report)

		switch se.Kind {
		case repo.KindDir:
			if err := os.Mkdir(dst, 0o755); err != nil {
				return failJob(RestoreStageEntries, se.RelPath, err)
			}
			createdDirs = append(createdDirs, dst)
			dirs++
		case repo.KindSymlink:
			if err := validateLinkTarget(job.Target, se.RelPath, se.LinkTarget); err != nil {
				return failJob(RestoreStageEntries, se.RelPath, err)
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return failJob(RestoreStageEntries, se.RelPath, err)
			}
			if err := os.Symlink(se.LinkTarget, dst); err != nil {
				if errors.Is(err, fs.ErrExist) {
					err = fmt.Errorf("refusing to overwrite existing file at %s", se.RelPath)
				}
				return failJob(RestoreStageEntries, se.RelPath, err)
			}
			symlinks++
		case repo.KindFile:
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return failJob(RestoreStageEntries, se.RelPath, err)
			}
			fr, rerr := restoreFileFrom(se, dst, func(digest []byte) (io.ReadCloser, error) {
				if e.Fail.BeforeRestoreOpenChunk != nil {
					e.Fail.BeforeRestoreOpenChunk(job.SnapshotID, se.RelPath, append([]byte(nil), digest...))
				}
				rc, err := e.Store.Open(digest)
				if err != nil {
					return nil, fmt.Errorf("%s: chunk %s: %w",
						se.RelPath, hex.EncodeToString(digest), err)
				}
				return rc, nil
			})
			if rerr != nil {
				return failJob(RestoreStageEntries, se.RelPath, rerr)
			}
			report.Size = fr.Size
			report.ChunkCount = fr.ChunkCount
			files++
			bytes += fr.Size
		default:
			err := fmt.Errorf("unknown entry kind %q", se.Kind)
			return failJob(RestoreStageEntries, se.RelPath, err)
		}
		report.Status = repo.RestoreEntryVerified
		report.Error = ""
		if err := e.Manifest.UpdateRestoreJobEntry(id, report); err != nil {
			return failJob(RestoreStageEntries, se.RelPath, err)
		}
	}

	// Apply non-directory metadata first, then directory modes/mtimes bottom
	// up. During construction directories are deliberately left writable.
	for _, se := range plan.entries {
		if se.RelPath == "." || se.Kind == repo.KindDir {
			continue
		}
		dst, _ := safeJoin(staging, se.RelPath)
		if se.Kind == repo.KindSymlink {
			if se.UID >= 0 {
				lchown(dst, se.UID, se.GID)
			}
			continue
		}
		chmod(dst, se.Mode)
		if se.UID >= 0 {
			chown(dst, se.UID, se.GID)
		}
		if err := os.Chtimes(dst, se.ModTime, se.ModTime); err != nil {
			return failJob(RestoreStageMetadata, se.RelPath, err)
		}
	}
	for i := len(createdDirs) - 1; i >= 0; i-- {
		_ = os.Chmod(createdDirs[i], 0o755)
	}
	modeByPath := map[string]repo.StoredEntry{}
	for _, se := range plan.entries {
		modeByPath[se.RelPath] = se
	}
	for i := len(createdDirs) - 1; i >= 0; i-- {
		d := createdDirs[i]
		rel, _ := filepath.Rel(staging, d)
		se := modeByPath[filepath.ToSlash(rel)]
		chmod(d, se.Mode)
		if se.UID >= 0 {
			chown(d, se.UID, se.GID)
		}
		if err := os.Chtimes(d, se.ModTime, se.ModTime); err != nil {
			return failJob(RestoreStageMetadata, filepath.ToSlash(rel), err)
		}
	}
	chmod(staging, plan.root.Mode)
	if plan.root.UID >= 0 {
		chown(staging, plan.root.UID, plan.root.GID)
	}
	if err := os.Chtimes(staging, plan.root.ModTime, plan.root.ModTime); err != nil {
		return failJob(RestoreStageMetadata, "", err)
	}
	if err := syncDir(staging); err != nil {
		return failJob(RestoreStagePublish, "", err)
	}

	// Re-check immediately before publication; preflight cannot hold an
	// indefinite lock over the absent target path.
	if _, err := os.Lstat(job.Target); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = fmt.Errorf("%w: %s", ErrTargetExists, job.Target)
		}
		return failJob(RestoreStagePublish, "", err)
	}
	if err := renameNoReplace(staging, job.Target); err != nil {
		if errors.Is(err, fs.ErrExist) || errors.Is(err, syscall.EEXIST) {
			err = fmt.Errorf("%w: %s", ErrTargetExists, job.Target)
		}
		return failJob(RestoreStagePublish, "", err)
	}
	cleanup = false
	_ = syncDir(filepath.Dir(job.Target))
	if err := e.Manifest.CompleteRestoreJob(id, files, dirs, symlinks, bytes); err != nil {
		// The rename is durable, but the status update was interrupted.
		_ = e.Manifest.MarkPublishedRestoreJob(id, files, dirs, symlinks, bytes)
		return err
	}
	_ = failed
	return nil
}

func mustJobPaths(m *repo.Manifest, id int64) []string {
	p, err := m.RestoreJobPaths(id, repo.RestorePathRequested)
	if err != nil {
		return nil
	}
	return p
}

// RecoverSelectiveRestores reconciles jobs left by an earlier process.
// Completed and failed jobs only have their own private staging directory
// removed. A running job is either recognized as an already-published target,
// or queued for a clean retry after removing its private staging directory.
func (e *Engine) RecoverSelectiveRestores() error {
	e.restoreMu.Lock()
	defer e.restoreMu.Unlock()

	jobs, err := e.Manifest.ListRestoreJobsByStatus("")
	if err != nil {
		return err
	}
	var toStart []int64
	for _, job := range jobs {
		cleanupStagingSibling(job.Target, job.StagingPath)
		switch job.Status {
		case repo.RestoreStatusPending:
			toStart = append(toStart, job.ID)
		case repo.RestoreStatusRunning:
			if err := e.recoverRunningRestore(job); err != nil {
				return err
			}
			queued, _ := e.Manifest.GetRestoreJob(job.ID)
			if queued.Status == repo.RestoreStatusPending {
				toStart = append(toStart, job.ID)
			}
		}
	}
	for _, id := range toStart {
		if _, ok := e.restoreJobs[id]; !ok {
			e.restoreJobs[id] = struct{}{}
			go func(jobID int64) {
				defer func() {
					e.restoreMu.Lock()
					delete(e.restoreJobs, jobID)
					e.restoreMu.Unlock()
				}()
				_ = e.runSelectiveRestore(jobID)
			}(id)
		}
	}
	return nil
}

func (e *Engine) recoverRunningRestore(job repo.RestoreJobInfo) error {
	fi, err := os.Lstat(job.Target)
	if errors.Is(err, os.ErrNotExist) {
		return e.Manifest.QueueInterruptedRestoreJob(job.ID)
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return e.Manifest.FailRestoreJob(job.ID, RestoreStageRecover, "",
			"published target exists but is not a directory; refusing to alter it")
	}
	reports, err := e.Manifest.ListRestoreJobEntries(job.ID)
	if err != nil {
		return err
	}
	var files, dirs, symlinks int
	var bytes int64
	for _, report := range reports {
		p := filepath.Join(job.Target, filepath.FromSlash(report.RelPath))
		info, statErr := os.Lstat(p)
		if statErr != nil {
			_ = e.Manifest.FailRestoreJob(job.ID, RestoreStageRecover, report.RelPath,
				"published target is incomplete: "+statErr.Error())
			return nil
		}
		switch report.Kind {
		case repo.KindDir:
			if !info.IsDir() || info.Mode().Perm() != os.FileMode(report.Mode).Perm() ||
				!info.ModTime().Equal(report.ModTime) {
				_ = e.Manifest.FailRestoreJob(job.ID, RestoreStageRecover, report.RelPath,
					"published directory metadata cannot be verified")
				return nil
			}
			dirs++
		case repo.KindSymlink:
			target, readErr := os.Readlink(p)
			if readErr != nil || target != report.LinkTarget {
				_ = e.Manifest.FailRestoreJob(job.ID, RestoreStageRecover, report.RelPath,
					"published symlink target cannot be verified")
				return nil
			}
			symlinks++
		case repo.KindFile:
			if !info.Mode().IsRegular() || info.Size() != report.Size ||
				info.Mode().Perm() != os.FileMode(report.Mode).Perm() ||
				!info.ModTime().Equal(report.ModTime) {
				_ = e.Manifest.FailRestoreJob(job.ID, RestoreStageRecover, report.RelPath,
					"published file metadata cannot be verified")
				return nil
			}
			digest, hashErr := hashRegularFile(p)
			if hashErr != nil || !equalBytes(digest, report.FileDigest) {
				msg := "published file digest cannot be verified"
				if hashErr != nil {
					msg = hashErr.Error()
				}
				_ = e.Manifest.FailRestoreJob(job.ID, RestoreStageRecover, report.RelPath, msg)
				return nil
			}
			files++
			bytes += report.Size
		}
	}
	// The selective root itself is not represented in reports; validate that
	// its mode/mtime match the immutable snapshot root entry.
	rootEntries, err := e.Manifest.EntriesOf(job.SnapshotID)
	if err != nil {
		return err
	}
	root := findRoot(rootEntries)
	if root == nil {
		return fmt.Errorf("snapshot %d missing root entry", job.SnapshotID)
	}
	rootInfo, err := os.Lstat(job.Target)
	if err != nil {
		return err
	}
	if rootInfo.Mode().Perm() != os.FileMode(root.Mode).Perm() ||
		!rootInfo.ModTime().Equal(root.ModTime) {
		return e.Manifest.FailRestoreJob(job.ID, RestoreStageRecover, "",
			"published root metadata cannot be verified")
	}
	_ = e.Manifest.MarkPublishedRestoreJob(job.ID, files, dirs, symlinks, bytes)
	return nil
}

func hashRegularFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func isPrivateSibling(target, staging string) bool {
	target = filepath.Clean(target)
	staging = filepath.Clean(staging)
	if filepath.Dir(staging) != filepath.Dir(target) {
		return false
	}
	base := filepath.Base(staging)
	return strings.HasPrefix(base, ".increstore-job-")
}

func cleanupStagingSibling(target, staging string) {
	if strings.TrimSpace(staging) == "" {
		return
	}
	var p string
	if filepath.IsAbs(staging) {
		p = filepath.Clean(staging)
	} else {
		p = filepath.Join(filepath.Dir(target), filepath.Base(staging))
	}
	if !isPrivateSibling(target, p) {
		return
	}
	_ = os.RemoveAll(p)
}
