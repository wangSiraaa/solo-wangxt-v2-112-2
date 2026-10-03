package backup

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"incbackup/internal/repo"
)

// SelectiveRestoreRequest freezes one selective restore: a committed snapshot
// plus a normalized set of relative paths to materialize at target.
type SelectiveRestoreRequest struct {
	SnapshotID     int64
	Target         string
	Paths          []string
	IdempotencyKey string
}

// RestoreJobResult is a job row plus its per-file plan/report.
type RestoreJobResult struct {
	Job      repo.RestoreJob
	Files    []repo.RestoreJobFile
	Replayed bool // an existing job was returned for the same idempotency key
}

// ErrUnknownPath is a requested path that does not exist in the snapshot
// manifest; the job is rejected at precheck, before anything is created.
type ErrUnknownPath struct{ Path string }

func (e *ErrUnknownPath) Error() string {
	return fmt.Sprintf("path %q is not in the snapshot manifest", e.Path)
}

// ErrMissingLinkDeps names internal symlink targets that the selection
// forgot to include. The job is rejected at precheck; the fix is always to
// widen the selection, never to rewrite the stored link target.
type ErrMissingLinkDeps struct{ Paths []string }

func (e *ErrMissingLinkDeps) Error() string {
	return fmt.Sprintf("selected symlinks resolve to internal targets that are not selected; add them to the selection: %s",
		strings.Join(e.Paths, ", "))
}

// ErrIdempotencyConflict is an idempotency key reused with a different
// snapshot, target or path set.
type ErrIdempotencyConflict struct{ Key string }

func (e *ErrIdempotencyConflict) Error() string {
	return fmt.Sprintf("idempotency key %q was already used with a different request", e.Key)
}

// stagingPrefix marks directories this service creates next to the restore
// target. Recovery only ever removes staging dirs matching this scheme and
// recorded in a job row — never anything else.
const stagingPrefix = ".incbackup-restore-staging-"

// NormalizeRestorePaths cleans a requested path set: rejects absolute paths
// and ".." escapes, resolves "." and duplicate segments, dedupes and sorts.
// The result is what gets frozen into the job row.
func NormalizeRestorePaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, errors.New("paths is required: select at least one file, directory or symlink")
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, errors.New("empty path in selection")
		}
		if filepath.IsAbs(filepath.FromSlash(p)) {
			return nil, fmt.Errorf("absolute path in selection: %q", p)
		}
		clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(p)))
		if clean == ".." || strings.HasPrefix(clean, "../") {
			return nil, fmt.Errorf("path escapes snapshot root: %q", p)
		}
		if !seen[clean] {
			seen[clean] = true
			out = append(out, clean)
		}
	}
	sort.Strings(out)
	return out, nil
}

// restorePlan is the expanded selection: exactly the manifest entries the
// job will materialize, in manifest order (parents before children).
type restorePlan struct {
	entries  []repo.StoredEntry
	implicit map[string]bool // auto-added parent directories
}

func parentDir(rel string) string {
	if rel == "." {
		return ""
	}
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		return rel[:i]
	}
	return "."
}

// planSelection expands the frozen path set against the snapshot manifest:
// directories bring their whole subtree, files and symlinks come alone, and
// every ancestor directory of an included path is auto-added. Selected
// symlinks are then prechecked: a link whose internal target exists in the
// manifest but is not part of the restored tree fails the whole job before
// anything is created. Link targets are never rewritten.
func planSelection(entries []repo.StoredEntry, requested []string, target string) (*restorePlan, error) {
	index := make(map[string]*repo.StoredEntry, len(entries))
	for i := range entries {
		index[entries[i].RelPath] = &entries[i]
	}
	include := map[string]bool{".": true}
	for _, p := range requested {
		e, ok := index[p]
		if !ok {
			return nil, &ErrUnknownPath{Path: p}
		}
		if e.Kind == repo.KindDir {
			prefix := ""
			if p != "." {
				prefix = p + "/"
			}
			for _, cand := range entries {
				if cand.RelPath == p || strings.HasPrefix(cand.RelPath, prefix) {
					include[cand.RelPath] = true
				}
			}
			continue
		}
		include[p] = true
	}
	// Auto-add every ancestor directory; each walk covers the full chain, so
	// ordering of the map iteration does not matter.
	implicit := map[string]bool{}
	for rel := range include {
		for d := parentDir(rel); d != "" && d != "."; d = parentDir(d) {
			if include[d] {
				break // inclusion of d already implies its ancestors
			}
			if _, ok := index[d]; !ok {
				return nil, fmt.Errorf("manifest is missing parent directory %q of %q", d, rel)
			}
			include[d] = true
			implicit[d] = true
		}
	}
	// Symlink dependency precheck.
	var missing []string
	for i := range entries {
		se := &entries[i]
		if se.Kind != repo.KindSymlink || !include[se.RelPath] {
			continue
		}
		resolved, ok := resolveUnderTarget(target, se.RelPath, se.LinkTarget)
		if !ok {
			return nil, fmt.Errorf("symlink %q -> %q escapes restore root", se.RelPath, se.LinkTarget)
		}
		if resolved == "." || include[resolved] {
			continue
		}
		if _, exists := index[resolved]; !exists {
			continue // dangling link: nothing to restore, same as a full restore
		}
		missing = append(missing, resolved)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, &ErrMissingLinkDeps{Paths: dedupStrings(missing)}
	}
	plan := &restorePlan{implicit: implicit}
	for i := range entries {
		if include[entries[i].RelPath] {
			plan.entries = append(plan.entries, entries[i])
		}
	}
	return plan, nil
}

// resolveUnderTarget resolves a symlink target lexically, exactly like
// validateLinkTarget, and returns the manifest-relative path it points at.
// ok is false when the link escapes the restore root.
func resolveUnderTarget(target, linkRel, linkTo string) (rel string, ok bool) {
	p := filepath.FromSlash(linkTo)
	var resolved string
	if filepath.IsAbs(p) {
		resolved = filepath.Clean(p)
	} else {
		resolved = filepath.Clean(filepath.Join(target, filepath.Dir(filepath.FromSlash(linkRel)), p))
	}
	if resolved == target {
		return ".", true
	}
	if !strings.HasPrefix(resolved, target+string(os.PathSeparator)) {
		return "", false
	}
	r, err := filepath.Rel(target, resolved)
	if err != nil {
		return "", false
	}
	return filepath.ToSlash(r), true
}

func dedupStrings(sorted []string) []string {
	out := sorted[:0]
	for i, s := range sorted {
		if i == 0 || s != sorted[i-1] {
			out = append(out, s)
		}
	}
	return out
}

func planFiles(plan *restorePlan) []repo.RestoreJobFile {
	var out []repo.RestoreJobFile
	for _, se := range plan.entries {
		if se.RelPath == "." {
			continue // the target root itself is not a report row
		}
		out = append(out, repo.RestoreJobFile{
			RelPath:  se.RelPath,
			Kind:     se.Kind,
			Implicit: plan.implicit[se.RelPath],
			Status:   repo.JobPending,
		})
	}
	return out
}

// ownsStagingPath guards every staging cleanup: the path must be the one the
// job row recorded, named by our scheme, and a sibling of the target.
func ownsStagingPath(job repo.RestoreJob) bool {
	return strings.HasPrefix(filepath.Base(job.Staging), stagingPrefix) &&
		filepath.Dir(job.Staging) == filepath.Dir(filepath.Clean(job.Target))
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// CreateRestoreJob freezes a selective-restore request into a persisted job
// and executes it. Repeating the request with the same idempotency key
// returns the same job without building a second tree.
func (e *Engine) CreateRestoreJob(req SelectiveRestoreRequest) (*RestoreJobResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if strings.TrimSpace(req.Target) == "" {
		return nil, errors.New("target is required")
	}
	paths, err := NormalizeRestorePaths(req.Paths)
	if err != nil {
		return nil, err
	}
	target, err := filepath.Abs(filepath.FromSlash(req.Target))
	if err != nil {
		return nil, err
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	if key == "" {
		key = "auto-" + randomHex(8)
	}

	// Idempotent replay comes first: a done job has already published its
	// target, so the not-exists precheck below must not run for it.
	if existing, err := e.Manifest.GetRestoreJobByKey(key); err == nil {
		if existing.SnapshotID != req.SnapshotID || existing.Target != target ||
			!equalStrings(existing.RequestedPaths, paths) {
			return nil, &ErrIdempotencyConflict{Key: key}
		}
		files, ferr := e.Manifest.RestoreJobFiles(existing.ID)
		if ferr != nil {
			return nil, ferr
		}
		return &RestoreJobResult{Job: existing, Files: files, Replayed: true}, nil
	} else if !errors.Is(err, repo.ErrJobNotFound) {
		return nil, err
	}

	info, err := e.Manifest.GetSnapshot(req.SnapshotID)
	if err != nil {
		return nil, err
	}
	if info.Status != repo.StatusCommitted {
		return nil, fmt.Errorf("snapshot %d is %s, only committed snapshots can be restored",
			req.SnapshotID, info.Status)
	}
	if err := noSymlinkAncestors(target); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return nil, fmt.Errorf("%w: %s", ErrTargetExists, target)
		}
		return nil, err
	}
	entries, err := e.Manifest.EntriesOf(req.SnapshotID)
	if err != nil {
		return nil, err
	}
	if err := validateEntryPaths(entries); err != nil {
		return nil, err
	}
	plan, err := planSelection(entries, paths, target)
	if err != nil {
		return nil, err // precheck failure: no job row, nothing on disk
	}

	job := repo.RestoreJob{
		IdempotencyKey: key,
		SnapshotID:     req.SnapshotID,
		Target:         target,
		Staging:        filepath.Join(filepath.Dir(target), stagingPrefix+randomHex(8)),
		RequestedPaths: paths,
	}
	id, err := e.Manifest.InsertRestoreJob(&job)
	if err != nil {
		return nil, err
	}
	job.ID = id
	if err := e.Manifest.ReplaceRestoreJobFiles(id, planFiles(plan)); err != nil {
		return nil, err
	}
	runErr := e.runRestoreJob(job)
	return e.jobResult(id, runErr)
}

// RetryRestoreJob re-executes a failed (or never-started) job with its frozen
// selection. A done job is returned unchanged; retrying never builds a
// second tree.
func (e *Engine) RetryRestoreJob(id int64) (*RestoreJobResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, err := e.Manifest.GetRestoreJob(id)
	if err != nil {
		return nil, err
	}
	switch job.Status {
	case repo.JobDone:
		return e.jobResult(id, nil)
	case repo.JobRunning:
		return nil, fmt.Errorf("restore job %d is running", id)
	}
	entries, err := e.Manifest.EntriesOf(job.SnapshotID)
	if err != nil {
		return nil, err
	}
	plan, err := planSelection(entries, job.RequestedPaths, job.Target)
	if err != nil {
		return nil, err
	}
	if err := e.Manifest.ReplaceRestoreJobFiles(id, planFiles(plan)); err != nil {
		return nil, err
	}
	runErr := e.runRestoreJob(job)
	return e.jobResult(id, runErr)
}

func (e *Engine) jobResult(id int64, runErr error) (*RestoreJobResult, error) {
	job, err := e.Manifest.GetRestoreJob(id)
	if err != nil {
		return nil, err
	}
	files, err := e.Manifest.RestoreJobFiles(id)
	if err != nil {
		return nil, err
	}
	return &RestoreJobResult{Job: job, Files: files}, runErr
}

// runRestoreJob builds the frozen plan in a private staging area next to the
// target, verifies every chunk and file as it streams, and only then
// atomically publishes the tree. Any failure cleans the staging area up and
// leaves the job failed with a per-file report that names the culprit.
func (e *Engine) runRestoreJob(job repo.RestoreJob) error {
	fail := func(cause error) error {
		_ = e.Manifest.UpdateRestoreJobStatus(job.ID, repo.JobFailed, cause.Error())
		return cause
	}
	failAt := func(rel string, cause error) error {
		if rel != "" {
			_ = e.Manifest.MarkRestoreJobFileFailed(job.ID, rel, cause.Error())
		}
		return fail(cause)
	}

	info, err := e.Manifest.GetSnapshot(job.SnapshotID)
	if err != nil {
		return fail(err)
	}
	if info.Status != repo.StatusCommitted {
		return fail(fmt.Errorf("snapshot %d is %s, only committed snapshots can be restored",
			job.SnapshotID, info.Status))
	}
	entries, err := e.Manifest.EntriesOf(job.SnapshotID)
	if err != nil {
		return fail(err)
	}
	plan, err := planSelection(entries, job.RequestedPaths, job.Target)
	if err != nil {
		return fail(err)
	}
	chunkLen, err := e.Manifest.ChunkLengthMap(job.SnapshotID)
	if err != nil {
		return fail(err)
	}
	if err := noSymlinkAncestors(job.Target); err != nil {
		return fail(err)
	}
	if _, err := os.Lstat(job.Target); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fail(fmt.Errorf("%w: %s", ErrTargetExists, job.Target))
		}
		return fail(err)
	}
	if err := e.Manifest.UpdateRestoreJobStatus(job.ID, repo.JobRunning, ""); err != nil {
		return err
	}

	staging := job.Staging
	if !ownsStagingPath(job) {
		return fail(fmt.Errorf("staging path %q fails the ownership check", staging))
	}
	if err := os.RemoveAll(staging); err != nil { // clear a previous attempt's staging
		return fail(fmt.Errorf("clean staging area: %w", err))
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return fail(err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			os.RemoveAll(staging) // never leave a half-built tree behind
		}
	}()

	var createdDirs []string
	for i := range plan.entries {
		se := &plan.entries[i]
		if se.RelPath == "." {
			continue // the staging dir itself carries the root entry's metadata
		}
		dst, err := safeJoin(staging, se.RelPath)
		if err != nil {
			return fail(err)
		}
		switch se.Kind {
		case repo.KindDir:
			// Owner-writable while the tree is populated; the exact mode is
			// applied in the finalize pass below.
			if err := os.Mkdir(dst, os.FileMode(se.Mode).Perm()|0o700); err != nil {
				return failAt(se.RelPath, fmt.Errorf("mkdir %s: %w", se.RelPath, err))
			}
			createdDirs = append(createdDirs, dst)
			_ = e.Manifest.MarkRestoreJobFileDone(job.ID, se.RelPath, 0, "", 0)
		case repo.KindSymlink:
			if err := validateLinkTarget(staging, se.RelPath, se.LinkTarget); err != nil {
				return failAt(se.RelPath, err)
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return failAt(se.RelPath, err)
			}
			// The stored target string is recreated verbatim, never rewritten.
			if err := os.Symlink(se.LinkTarget, dst); err != nil {
				return failAt(se.RelPath, fmt.Errorf("symlink %s: %w", se.RelPath, err))
			}
			_ = e.Manifest.MarkRestoreJobFileDone(job.ID, se.RelPath, 0, "", 0)
		case repo.KindFile:
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return failAt(se.RelPath, err)
			}
			rep, err := e.restoreFileChecked(*se, dst, chunkLen)
			if err != nil {
				return failAt(se.RelPath, fmt.Errorf("restore %s: %w", se.RelPath, err))
			}
			_ = e.Manifest.MarkRestoreJobFileDone(job.ID, se.RelPath, rep.Size, rep.Digest, rep.ChunkCount)
		default:
			return failAt(se.RelPath, fmt.Errorf("unknown entry kind %q", se.Kind))
		}
	}

	// Metadata: files/symlinks first, then directories bottom-up so
	// restrictive modes cannot block their own children.
	for i := range plan.entries {
		se := &plan.entries[i]
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
		os.Chtimes(dst, se.ModTime, se.ModTime)
	}
	for i := len(createdDirs) - 1; i >= 0; i-- {
		d := createdDirs[i]
		if se := findDir(plan.entries, d, staging); se != nil {
			chmod(d, se.Mode)
			if se.UID >= 0 {
				chown(d, se.UID, se.GID)
			}
			os.Chtimes(d, se.ModTime, se.ModTime)
		} else {
			chmod(d, 0o755)
		}
	}
	if root := findRoot(plan.entries); root != nil {
		chmod(staging, root.Mode)
		if root.UID >= 0 {
			chown(staging, root.UID, root.GID)
		}
		os.Chtimes(staging, root.ModTime, root.ModTime)
	}

	// Everything restored and verified: publish atomically, still refusing
	// to overwrite anything that appeared at the target in the meantime.
	if err := renameNoReplace(staging, job.Target); err != nil {
		return fail(fmt.Errorf("publish to %s: %w", job.Target, err))
	}
	cleanup = false
	return e.Manifest.UpdateRestoreJobStatus(job.ID, repo.JobDone, "")
}

// restoreFileChecked is restoreFile plus a per-chunk length check against the
// catalog: every chunk must have a catalog row, stream exactly that many
// bytes, and match its content digest (verified by the store reader); the
// assembled file is then checked against the manifest's length and SHA-256.
func (e *Engine) restoreFileChecked(se repo.StoredEntry, dst string, chunkLen map[string]int64) (FileReport, error) {
	f, err := openExclusiveFile(dst, os.FileMode(se.Mode).Perm())
	if err != nil {
		return FileReport{}, err
	}
	h := newFileHasher()
	var total int64
	var count int
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(dst)
		}
	}()

	for _, d := range se.ChunkDigests {
		want, ok := chunkLen[string(d)]
		if !ok {
			return FileReport{}, fmt.Errorf("chunk %x has no catalog row (commit interrupted)", d)
		}
		rc, err := e.Store.Open(d) // verifies chunk digest while streaming
		if err != nil {
			return FileReport{}, fmt.Errorf("chunk %x: %w", d, err)
		}
		n, err := copyChunks(f, rc, h)
		rc.Close()
		if err != nil {
			return FileReport{}, fmt.Errorf("chunk %x: %w", d, err)
		}
		if n != want {
			return FileReport{}, fmt.Errorf("chunk %x length mismatch: restored %d bytes, catalog says %d", d, n, want)
		}
		total += n
		count++
	}
	if err := f.Sync(); err != nil {
		return FileReport{}, err
	}
	if err := f.Close(); err != nil {
		return FileReport{}, err
	}
	if total != se.Size {
		return FileReport{}, fmt.Errorf("length mismatch: restored %d bytes, manifest says %d", total, se.Size)
	}
	got := h.checksum()
	if se.FileDigest != nil && !equalBytes(got, se.FileDigest) {
		return FileReport{}, fmt.Errorf("digest mismatch: restored %x, manifest %x", got, se.FileDigest)
	}
	ok = true
	return FileReport{
		RelPath:    se.RelPath,
		Size:       total,
		Digest:     hex.EncodeToString(got),
		Mode:       os.FileMode(se.Mode).Perm(),
		ChunkCount: count,
	}, nil
}

// RecoverRestoreJobs reconciles jobs abandoned by a crash. It only ever
// removes staging areas it created itself (recorded in the job row and
// matching the staging naming scheme). A job whose atomic publish may have
// completed is continued by verifying the published tree against the
// manifest; a job interrupted mid-staging is cleaned up and marked failed so
// a retry can rebuild it from scratch.
func (e *Engine) RecoverRestoreJobs() ([]repo.RestoreJob, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []repo.RestoreJob

	interrupted, err := e.Manifest.RestoreJobsWithStatus(repo.JobRunning)
	if err != nil {
		return nil, err
	}
	for _, job := range interrupted {
		_, stErr := os.Lstat(job.Staging)
		switch {
		case stErr == nil:
			// Crashed while building the staging tree: remove our own staging
			// area, and only if it is verifiably ours.
			if ownsStagingPath(job) {
				if err := os.RemoveAll(job.Staging); err != nil {
					return out, fmt.Errorf("clean staging of job %d: %w", job.ID, err)
				}
			}
			e.setJobFailed(job.ID, "interrupted by service restart; staging area cleaned, retry to resume")
		case errors.Is(stErr, os.ErrNotExist):
			if _, tErr := os.Lstat(job.Target); tErr == nil {
				// Crashed between the atomic publish and the status update:
				// continue by verifying what was published.
				if err := e.verifyPublishedJob(job); err != nil {
					e.setJobFailed(job.ID, "published tree failed verification after restart: "+err.Error())
				} else {
					_ = e.Manifest.UpdateRestoreJobStatus(job.ID, repo.JobDone, "")
				}
			} else {
				e.setJobFailed(job.ID, "interrupted by service restart before publish; retry to resume")
			}
		default:
			e.setJobFailed(job.ID, "restart recovery could not inspect staging area: "+stErr.Error())
		}
		if j, err := e.Manifest.GetRestoreJob(job.ID); err == nil {
			out = append(out, j)
		}
	}

	// Pending jobs never started; continue them now.
	pending, err := e.Manifest.RestoreJobsWithStatus(repo.JobPending)
	if err != nil {
		return out, err
	}
	for _, job := range pending {
		_ = e.runRestoreJob(job)
		if j, err := e.Manifest.GetRestoreJob(job.ID); err == nil {
			out = append(out, j)
		}
	}
	return out, nil
}

func (e *Engine) setJobFailed(id int64, msg string) {
	_ = e.Manifest.UpdateRestoreJobStatus(id, repo.JobFailed, msg)
}

// verifyPublishedJob re-verifies an already-published tree against the frozen
// selection: every planned entry must exist at target with the right kind,
// link targets must match the manifest verbatim, and every file must hash to
// the manifest's digest and length.
func (e *Engine) verifyPublishedJob(job repo.RestoreJob) error {
	entries, err := e.Manifest.EntriesOf(job.SnapshotID)
	if err != nil {
		return err
	}
	plan, err := planSelection(entries, job.RequestedPaths, job.Target)
	if err != nil {
		return err
	}
	for i := range plan.entries {
		se := &plan.entries[i]
		dst, err := safeJoin(job.Target, se.RelPath)
		if err != nil {
			return err
		}
		fi, err := os.Lstat(dst)
		if err != nil {
			return fmt.Errorf("%s: %w", se.RelPath, err)
		}
		switch se.Kind {
		case repo.KindDir:
			if !fi.IsDir() {
				return fmt.Errorf("%s: not a directory", se.RelPath)
			}
		case repo.KindSymlink:
			tgt, err := os.Readlink(dst)
			if err != nil {
				return fmt.Errorf("%s: %w", se.RelPath, err)
			}
			if tgt != se.LinkTarget {
				return fmt.Errorf("%s: link target %q, manifest says %q", se.RelPath, tgt, se.LinkTarget)
			}
		case repo.KindFile:
			if !fi.Mode().IsRegular() {
				return fmt.Errorf("%s: not a regular file", se.RelPath)
			}
			sum, n, err := hashFilePath(dst)
			if err != nil {
				return fmt.Errorf("%s: %w", se.RelPath, err)
			}
			if n != se.Size {
				return fmt.Errorf("%s: length mismatch: %d on disk, manifest says %d", se.RelPath, n, se.Size)
			}
			if se.FileDigest != nil && !equalBytes(sum, se.FileDigest) {
				return fmt.Errorf("%s: digest mismatch: %x on disk, manifest %x", se.RelPath, sum, se.FileDigest)
			}
		}
	}
	return nil
}

func hashFilePath(p string) ([]byte, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), n, nil
}
