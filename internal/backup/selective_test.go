package backup_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// snapshotOf writes files+modes under dir/src and commits a snapshot of it.
func snapshotOf(t *testing.T, e *backup.Engine, dir string, files map[string]string, modes map[string]os.FileMode) int64 {
	t.Helper()
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	writeTree(t, src, files, modes)
	res, err := e.CreateSnapshot(src, "selective-test", true)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if res.Status != repo.StatusCommitted {
		t.Fatalf("status=%s", res.Status)
	}
	return res.SnapshotID
}

func jobFilesByRel(t *testing.T, e *backup.Engine, jobID int64) map[string]repo.RestoreJobFile {
	t.Helper()
	files, err := e.Manifest.RestoreJobFiles(jobID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]repo.RestoreJobFile{}
	for _, f := range files {
		out[f.RelPath] = f
	}
	return out
}

func stagingLeftovers(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".incbackup-restore-staging-*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

// Acceptance ①: selecting one deep file restores only that file and the
// necessary parent directories, with correct digest, mode and mtime.
func TestSelectiveRestoreDeepFileOnly(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(filepath.Join(src, "a", "b", "c"), 0o755))
	deep := strings.Repeat("deep-content\n", 3000)
	writeTree(t, src, map[string]string{
		"a/b/c/deep.txt": deep,
		"a/b/other.txt":  "sibling\n",
		"top.txt":        "top\n",
	}, map[string]os.FileMode{"a/b": 0o750, "a/b/c/deep.txt": 0o640})
	must(t, os.Chmod(filepath.Join(src, "a", "b"), 0o750)) // writeTree only chmods files
	// pin mtimes so the check is exact
	mtime := time.Date(2026, 9, 15, 10, 30, 0, 0, time.UTC)
	for _, rel := range []string{"a/b", "a/b/c", "a/b/c/deep.txt"} {
		must(t, os.Chtimes(filepath.Join(src, filepath.FromSlash(rel)), mtime, mtime))
	}
	res, err := e.CreateSnapshot(src, "deep", true)
	if err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(dir, "partial")
	jr, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID:     res.SnapshotID,
		Target:         target,
		Paths:          []string{"a//b/./c/deep.txt", "a/b/c/deep.txt"}, // normalizes + dedupes
		IdempotencyKey: "k-deep",
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if jr.Job.Status != repo.JobDone {
		t.Fatalf("job status=%s err=%s", jr.Job.Status, jr.Job.Error)
	}
	if got := jr.Job.RequestedPaths; len(got) != 1 || got[0] != "a/b/c/deep.txt" {
		t.Fatalf("frozen paths not normalized: %v", got)
	}

	// plan = exactly the file plus its ancestor directories
	want := map[string]bool{
		"a": false, "a/b": false, "a/b/c": false, "a/b/c/deep.txt": true,
	}
	if len(jr.Files) != len(want) {
		t.Fatalf("plan has %d rows, want %d: %+v", len(jr.Files), len(want), jr.Files)
	}
	for _, f := range jr.Files {
		isFile, ok := want[f.RelPath]
		if !ok {
			t.Fatalf("unexpected plan row %q", f.RelPath)
		}
		if f.Status != repo.JobDone {
			t.Fatalf("plan row %s status=%s", f.RelPath, f.Status)
		}
		if f.Implicit == isFile {
			t.Fatalf("%s implicit=%v, want %v", f.RelPath, f.Implicit, !isFile)
		}
	}

	// only the selected file exists on disk; siblings were not restored
	if _, err := os.Lstat(filepath.Join(target, "a", "b", "other.txt")); !os.IsNotExist(err) {
		t.Fatal("unselected sibling a/b/other.txt must not be restored")
	}
	if _, err := os.Lstat(filepath.Join(target, "top.txt")); !os.IsNotExist(err) {
		t.Fatal("unselected top.txt must not be restored")
	}

	// digest + length verified against an independent recomputation
	gotSum, gotLen := fileSHA(t, filepath.Join(target, "a", "b", "c", "deep.txt"))
	if gotSum != sha256Hex([]byte(deep)) || gotLen != int64(len(deep)) {
		t.Fatalf("restored content mismatch: %s/%d", gotSum, gotLen)
	}
	rep := jobFilesByRel(t, e, jr.Job.ID)["a/b/c/deep.txt"]
	if rep.Digest != gotSum || rep.Size != gotLen || rep.ChunkCount < 1 {
		t.Fatalf("report row: %+v vs disk %s/%d", rep, gotSum, gotLen)
	}

	// mode + mtime of file and auto-added parent dirs
	fi, err := os.Lstat(filepath.Join(target, "a", "b", "c", "deep.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o640 {
		t.Fatalf("deep.txt mode=%o want 640", fi.Mode().Perm())
	}
	if !fi.ModTime().Equal(mtime) {
		t.Fatalf("deep.txt mtime=%v want %v", fi.ModTime(), mtime)
	}
	di, err := os.Lstat(filepath.Join(target, "a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o750 {
		t.Fatalf("a/b mode=%o want 750", di.Mode().Perm())
	}
	if !di.ModTime().Equal(mtime) {
		t.Fatalf("a/b mtime=%v want %v", di.ModTime(), mtime)
	}
}

// Selecting a directory restores its whole subtree, but nothing outside it.
func TestSelectiveRestoreDirectorySubtree(t *testing.T) {
	e, dir := openEngine(t)
	snapID := snapshotOf(t, e, dir, map[string]string{
		"a/b/f1.txt":   "one\n",
		"a/b/g/f2.txt": "two\n",
		"a/other.txt":  "other\n",
	}, nil)

	target := filepath.Join(dir, "subtree")
	jr, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: snapID, Target: target, Paths: []string{"a/b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if jr.Job.Status != repo.JobDone {
		t.Fatalf("status=%s err=%s", jr.Job.Status, jr.Job.Error)
	}
	for _, rel := range []string{"a/b/f1.txt", "a/b/g/f2.txt"} {
		if _, err := os.Lstat(filepath.Join(target, filepath.FromSlash(rel))); err != nil {
			t.Fatalf("%s missing: %v", rel, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(target, "a", "other.txt")); !os.IsNotExist(err) {
		t.Fatal("a/other.txt is outside the selected subtree")
	}
}

// Acceptance ②: a selected symlink whose internal target is not selected
// fails at precheck, before anything is created; including the dependency
// fixes it, and the stored link target is recreated verbatim.
func TestSelectiveRestoreMissingLinkTargetFailsPrecheck(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(filepath.Join(src, "docs"), 0o755))
	writeTree(t, src, map[string]string{"docs/notes.txt": "notes\n"}, nil)
	must(t, os.Symlink("docs/notes.txt", filepath.Join(src, "link_to_notes")))
	res, err := e.CreateSnapshot(src, "links", true)
	if err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(dir, "partial-link")
	_, err = e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID:     res.SnapshotID,
		Target:         target,
		Paths:          []string{"link_to_notes"},
		IdempotencyKey: "k-link",
	})
	var deps *backup.ErrMissingLinkDeps
	if !errors.As(err, &deps) {
		t.Fatalf("want ErrMissingLinkDeps, got %v", err)
	}
	if len(deps.Paths) != 1 || deps.Paths[0] != "docs/notes.txt" {
		t.Fatalf("missing deps = %v", deps.Paths)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatal("precheck failure must not create the target directory")
	}
	if left := stagingLeftovers(t, dir); len(left) != 0 {
		t.Fatalf("precheck failure must not leave staging dirs: %v", left)
	}
	// a failed precheck persists no job row
	if _, err := e.Manifest.GetRestoreJobByKey("k-link"); !errors.Is(err, repo.ErrJobNotFound) {
		t.Fatalf("precheck failure must not persist a job, got %v", err)
	}

	// include the dependency: the job succeeds and the link is verbatim
	jr, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID:     res.SnapshotID,
		Target:         target,
		Paths:          []string{"link_to_notes", "docs/notes.txt"},
		IdempotencyKey: "k-link",
	})
	if err != nil {
		t.Fatalf("with dependency included: %v", err)
	}
	if jr.Job.Status != repo.JobDone {
		t.Fatalf("status=%s err=%s", jr.Job.Status, jr.Job.Error)
	}
	lt, err := os.Readlink(filepath.Join(target, "link_to_notes"))
	if err != nil || lt != "docs/notes.txt" {
		t.Fatalf("link target = %q, %v (must be recreated verbatim)", lt, err)
	}
	if got, _ := fileSHA(t, filepath.Join(target, "docs", "notes.txt")); got != sha256Hex([]byte("notes\n")) {
		t.Fatal("notes.txt content mismatch")
	}
}

// A selected symlink that escapes the restore root is rejected, and a
// dangling internal link (target not in the manifest) restores fine.
func TestSelectiveRestoreLinkSafety(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	writeTree(t, src, map[string]string{"f.txt": "data\n"}, nil)
	outside := filepath.Join(dir, "secret.txt")
	must(t, os.WriteFile(outside, []byte("secret"), 0o600))
	rel, _ := filepath.Rel(src, outside)
	must(t, os.Symlink(rel, filepath.Join(src, "escape")))
	must(t, os.Symlink("no/such/file", filepath.Join(src, "dangling")))
	res, err := e.CreateSnapshot(src, "links", true)
	if err != nil {
		t.Fatal(err)
	}

	_, err = e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: res.SnapshotID, Target: filepath.Join(dir, "p1"), Paths: []string{"escape"},
	})
	if err == nil || !strings.Contains(err.Error(), "escapes restore root") {
		t.Fatalf("escaping link must be rejected, got %v", err)
	}

	jr, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: res.SnapshotID, Target: filepath.Join(dir, "p2"), Paths: []string{"dangling"},
	})
	if err != nil {
		t.Fatalf("dangling link must restore: %v", err)
	}
	if jr.Job.Status != repo.JobDone {
		t.Fatalf("status=%s", jr.Job.Status)
	}
	lt, _ := os.Readlink(filepath.Join(dir, "p2", "dangling"))
	if lt != "no/such/file" {
		t.Fatalf("dangling link target = %q", lt)
	}
}

// Acceptance ③: the same idempotency key returns the same job and never
// builds a second tree; a different request to an existing target conflicts
// and leaves nothing behind.
func TestSelectiveRestoreIdempotency(t *testing.T) {
	e, dir := openEngine(t)
	snapID := snapshotOf(t, e, dir, map[string]string{"f.txt": "payload\n"}, nil)
	target := filepath.Join(dir, "out")

	req := backup.SelectiveRestoreRequest{
		SnapshotID: snapID, Target: target, Paths: []string{"f.txt"}, IdempotencyKey: "k-1",
	}
	first, err := e.CreateRestoreJob(req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Job.Status != repo.JobDone {
		t.Fatalf("status=%s", first.Job.Status)
	}

	// replay: same key, same request -> same job, no new tree
	second, err := e.CreateRestoreJob(req)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed {
		t.Fatal("second request must be an idempotent replay")
	}
	if second.Job.ID != first.Job.ID {
		t.Fatalf("replay returned job %d, want %d", second.Job.ID, first.Job.ID)
	}
	jobs, err := e.Manifest.ListRestoreJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("%d job rows, want 1", len(jobs))
	}

	// same key, different request -> conflict
	_, err = e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: snapID, Target: target, Paths: []string{"f.txt", "."}, IdempotencyKey: "k-1",
	})
	var idem *backup.ErrIdempotencyConflict
	if !errors.As(err, &idem) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}

	// different key, existing target -> conflict, nothing left behind
	_, err = e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: snapID, Target: target, Paths: []string{"f.txt"}, IdempotencyKey: "k-2",
	})
	if !errors.Is(err, backup.ErrTargetExists) {
		t.Fatalf("want ErrTargetExists, got %v", err)
	}
	if left := stagingLeftovers(t, dir); len(left) != 0 {
		t.Fatalf("conflict must not leave staging dirs: %v", left)
	}
	if jobs, _ := e.Manifest.ListRestoreJobs(); len(jobs) != 1 {
		t.Fatalf("conflicting request must not persist a job; have %d", len(jobs))
	}

	// retrying a done job is a no-op returning the same job
	again, err := e.RetryRestoreJob(first.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Job.Status != repo.JobDone || again.Job.ID != first.Job.ID {
		t.Fatalf("retry of done job: %+v", again.Job)
	}
}

// Acceptance ④: a corrupted chunk of a selected file fails the job with a
// per-file report that locates the bad chunk; after repair the job retries
// to done. Other committed snapshots still restore in full.
func TestSelectiveRestoreChunkCorruptionAndRetry(t *testing.T) {
	e, dir := openEngine(t)

	// baseline snapshot that must stay restorable throughout
	baseID := snapshotOf(t, e, dir, map[string]string{"base.txt": "baseline\n"}, nil)

	// second tree with a unique multi-chunk file
	src2 := filepath.Join(dir, "src2")
	must(t, os.MkdirAll(src2, 0o755))
	payload := strings.Repeat("unique-payload-", 8000)
	must(t, os.WriteFile(filepath.Join(src2, "unique.bin"), []byte(payload), 0o644))
	res, err := e.CreateSnapshot(src2, "victim", true)
	if err != nil {
		t.Fatal(err)
	}

	// corrupt the first chunk of unique.bin in the store
	entries, err := e.Manifest.EntriesOf(res.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	var digest []byte
	for _, se := range entries {
		if se.RelPath == "unique.bin" {
			digest = se.ChunkDigests[0]
		}
	}
	if digest == nil {
		t.Fatal("unique.bin chunks not found")
	}
	blob, err := e.Store.Path(digest)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	must(t, os.Chmod(blob, 0o644))
	corrupt := append([]byte(nil), original...)
	corrupt[0] ^= 0xFF
	must(t, os.WriteFile(blob, corrupt, 0o444))

	target := filepath.Join(dir, "partial")
	jr, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID:     res.SnapshotID,
		Target:         target,
		Paths:          []string{"unique.bin"},
		IdempotencyKey: "k-corrupt",
	})
	if err == nil {
		t.Fatal("corrupted chunk must fail the job")
	}
	if jr == nil || jr.Job.Status != repo.JobFailed {
		t.Fatalf("job must be persisted as failed: %+v", jr)
	}
	if !strings.Contains(jr.Job.Error, "unique.bin") {
		t.Fatalf("job error must name the file: %q", jr.Job.Error)
	}
	rep := jobFilesByRel(t, e, jr.Job.ID)["unique.bin"]
	if rep.Status != repo.JobFailed {
		t.Fatalf("file row status=%s", rep.Status)
	}
	if !strings.Contains(rep.Error, fmt.Sprintf("%x", digest)[:16]) {
		t.Fatalf("file error must locate the chunk digest %x: %q", digest, rep.Error)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatal("failed job must not leave the target behind")
	}
	if left := stagingLeftovers(t, dir); len(left) != 0 {
		t.Fatalf("failed job must clean its staging area: %v", left)
	}

	// repair the blob, then retry the same job
	must(t, os.Chmod(blob, 0o644))
	must(t, os.WriteFile(blob, original, 0o444))
	jr2, err := e.RetryRestoreJob(jr.Job.ID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if jr2.Job.Status != repo.JobDone {
		t.Fatalf("after repair status=%s err=%s", jr2.Job.Status, jr2.Job.Error)
	}
	gotSum, gotLen := fileSHA(t, filepath.Join(target, "unique.bin"))
	if gotSum != sha256Hex([]byte(payload)) || gotLen != int64(len(payload)) {
		t.Fatal("retried restore content mismatch")
	}

	// the untouched baseline snapshot still restores in full
	if _, err := e.Restore(baseID, filepath.Join(dir, "base-out")); err != nil {
		t.Fatalf("baseline full restore: %v", err)
	}
}

// A missing blob (service-side storage loss) is located by the job report
// just like a corrupted one.
func TestSelectiveRestoreMissingBlobIsLocated(t *testing.T) {
	e, dir := openEngine(t)
	snapID := snapshotOf(t, e, dir, map[string]string{"gone.bin": strings.Repeat("x", 9000)}, nil)

	entries, err := e.Manifest.EntriesOf(snapID)
	if err != nil {
		t.Fatal(err)
	}
	digest := entries[1].ChunkDigests[0] // entries[0] is "."
	must(t, e.Store.Remove(digest))

	jr, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: snapID, Target: filepath.Join(dir, "out"), Paths: []string{"gone.bin"},
	})
	if err == nil || jr.Job.Status != repo.JobFailed {
		t.Fatalf("want failed job, got %v %+v", err, jr)
	}
	rep := jobFilesByRel(t, e, jr.Job.ID)["gone.bin"]
	if rep.Status != repo.JobFailed || !strings.Contains(rep.Error, fmt.Sprintf("%x", digest)[:16]) {
		t.Fatalf("report must locate the missing chunk: %+v", rep)
	}
}

// Crash recovery: an interrupted running job's own staging area is cleaned
// up and the job becomes retryable; a job that crashed right after the
// atomic publish is continued by verifying the published tree.
func TestSelectiveRestoreCrashRecovery(t *testing.T) {
	e, dir := openEngine(t)
	snapID := snapshotOf(t, e, dir, map[string]string{"f.txt": "recover me\n"}, nil)

	// --- case 1: crash mid-staging ---
	jid, err := e.Manifest.InsertRestoreJob(&repo.RestoreJob{
		IdempotencyKey: "k-crash1",
		SnapshotID:     snapID,
		Target:         filepath.Join(dir, "t1"),
		Staging:        filepath.Join(dir, ".incbackup-restore-staging-cafe01"),
		RequestedPaths: []string{"f.txt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	must(t, e.Manifest.UpdateRestoreJobStatus(jid, repo.JobRunning, ""))
	must(t, os.MkdirAll(filepath.Join(dir, ".incbackup-restore-staging-cafe01", "junk"), 0o700))

	recovered, err := e.RecoverRestoreJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].Status != repo.JobFailed {
		t.Fatalf("recovered = %+v", recovered)
	}
	if _, err := os.Lstat(filepath.Join(dir, ".incbackup-restore-staging-cafe01")); !os.IsNotExist(err) {
		t.Fatal("interrupted staging area must be cleaned")
	}
	// retry completes the job
	jr, err := e.RetryRestoreJob(jid)
	if err != nil {
		t.Fatal(err)
	}
	if jr.Job.Status != repo.JobDone {
		t.Fatalf("status=%s err=%s", jr.Job.Status, jr.Job.Error)
	}
	if got, _ := fileSHA(t, filepath.Join(dir, "t1", "f.txt")); got != sha256Hex([]byte("recover me\n")) {
		t.Fatal("recovered job content mismatch")
	}

	// --- case 2: crash between atomic publish and status update ---
	jr2, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: snapID, Target: filepath.Join(dir, "t2"), Paths: []string{"f.txt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// rewind the done job to running, as if the crash hit after rename
	must(t, e.Manifest.UpdateRestoreJobStatus(jr2.Job.ID, repo.JobRunning, ""))
	recovered, err = e.RecoverRestoreJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].Status != repo.JobDone {
		t.Fatalf("published tree must be verified and marked done: %+v", recovered)
	}

	// --- case 3: a staging dir that is not ours is never touched ---
	stranger := filepath.Join(dir, "someone-elses-dir")
	must(t, os.MkdirAll(stranger, 0o755))
	jid3, err := e.Manifest.InsertRestoreJob(&repo.RestoreJob{
		IdempotencyKey: "k-crash3",
		SnapshotID:     snapID,
		Target:         filepath.Join(dir, "t3"),
		Staging:        stranger, // does not match our naming scheme
		RequestedPaths: []string{"f.txt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	must(t, e.Manifest.UpdateRestoreJobStatus(jid3, repo.JobRunning, ""))
	if _, err := e.RecoverRestoreJobs(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(stranger); err != nil {
		t.Fatal("recovery must not remove a staging dir it did not create")
	}
}

// A pending job (crash between insert and execution) is continued by
// recovery, not left dangling.
func TestSelectiveRestorePendingJobContinues(t *testing.T) {
	e, dir := openEngine(t)
	snapID := snapshotOf(t, e, dir, map[string]string{"f.txt": "pending\n"}, nil)
	jid, err := e.Manifest.InsertRestoreJob(&repo.RestoreJob{
		IdempotencyKey: "k-pending",
		SnapshotID:     snapID,
		Target:         filepath.Join(dir, "t"),
		Staging:        filepath.Join(dir, ".incbackup-restore-staging-pend01"),
		RequestedPaths: []string{"f.txt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := e.RecoverRestoreJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].Status != repo.JobDone || recovered[0].ID != jid {
		t.Fatalf("pending job must be continued to done: %+v", recovered)
	}
	if got, _ := fileSHA(t, filepath.Join(dir, "t", "f.txt")); got != sha256Hex([]byte("pending\n")) {
		t.Fatal("continued job content mismatch")
	}
}

// Path normalization rejects escapes and absolutes before anything else.
func TestSelectiveRestorePathValidation(t *testing.T) {
	e, dir := openEngine(t)
	snapID := snapshotOf(t, e, dir, map[string]string{"f.txt": "x\n"}, nil)
	for _, bad := range [][]string{
		{"/etc/passwd"},
		{"../escape"},
		{"a/../../escape"},
		{""},
		{},
	} {
		_, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
			SnapshotID: snapID, Target: filepath.Join(dir, "v"), Paths: bad,
		})
		if err == nil {
			t.Fatalf("paths %v must be rejected", bad)
		}
		var unknown *backup.ErrUnknownPath
		if errors.As(err, &unknown) {
			t.Fatalf("paths %v must fail normalization, not lookup: %v", bad, err)
		}
	}
	// unknown manifest path -> ErrUnknownPath
	_, err := e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: snapID, Target: filepath.Join(dir, "v"), Paths: []string{"nope.txt"},
	})
	var unknown *backup.ErrUnknownPath
	if !errors.As(err, &unknown) {
		t.Fatalf("want ErrUnknownPath, got %v", err)
	}
	// uncommitted snapshot cannot be selectively restored
	pend, err := e.CreateSnapshot(filepath.Join(dir, "src"), "pending", false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID: pend.SnapshotID, Target: filepath.Join(dir, "v2"), Paths: []string{"f.txt"},
	})
	if err == nil || !strings.Contains(err.Error(), "only committed snapshots") {
		t.Fatalf("pending snapshot must be rejected, got %v", err)
	}
}
