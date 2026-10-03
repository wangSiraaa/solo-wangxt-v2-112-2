package backup_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

func waitRestoreJob(t *testing.T, e *backup.Engine, id int64) repo.RestoreJobInfo {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := e.GetSelectiveRestore(id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == repo.RestoreStatusCompleted || j.Status == repo.RestoreStatusFailed {
			return *j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("restore job %d did not finish", id)
	return repo.RestoreJobInfo{}
}

func TestSelectiveRestoreDeepFileOnly(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(filepath.Join(src, "a", "b", "c"), 0o750))
	deep := filepath.Join(src, "a", "b", "c", "deep.txt")
	must(t, os.WriteFile(deep, []byte("deep secret\n"), 0o600))
	must(t, os.Chmod(filepath.Join(src, "a", "b"), 0o700))
	must(t, os.WriteFile(filepath.Join(src, "sibling.txt"), []byte("not selected\n"), 0o644))

	snap, err := e.CreateSnapshot(src, "selective", true)
	must(t, err)
	target := filepath.Join(dir, "selective-out")
	job, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target,
		Paths: []string{"./a/b/../b/c/deep.txt"}, IdempotencyKey: "deep",
	})
	must(t, err)
	finished := waitRestoreJob(t, e, job.ID)
	if finished.Status != repo.RestoreStatusCompleted {
		t.Fatalf("status=%s stage=%s rel=%s msg=%s",
			finished.Status, finished.Stage, finished.RelPath, finished.Message)
	}

	if _, err := os.Lstat(filepath.Join(target, "sibling.txt")); !os.IsNotExist(err) {
		t.Fatalf("unselected sibling must not be restored: %v", err)
	}
	for _, rel := range []string{"a", "a/b", "a/b/c"} {
		fi, err := os.Lstat(filepath.Join(target, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("required parent %s: %v", rel, err)
		}
		if !fi.IsDir() {
			t.Fatalf("%s must be a directory", rel)
		}
	}
	srcParent, err := os.Lstat(filepath.Join(src, "a", "b"))
	must(t, err)
	dstParent, err := os.Lstat(filepath.Join(target, "a", "b"))
	must(t, err)
	if srcParent.Mode().Perm() != dstParent.Mode().Perm() ||
		!srcParent.ModTime().Equal(dstParent.ModTime()) {
		t.Fatalf("parent metadata mismatch: %o %v vs %o %v",
			srcParent.Mode().Perm(), srcParent.ModTime(),
			dstParent.Mode().Perm(), dstParent.ModTime())
	}
	gotDigest, gotLen := fileSHA(t, deep)
	restoredDigest, restoredLen := fileSHA(t, filepath.Join(target, "a", "b", "c", "deep.txt"))
	if gotLen != restoredLen || gotDigest != restoredDigest {
		t.Fatalf("restored digest/length = %s/%d, source %s/%d", restoredDigest, restoredLen, gotDigest, gotLen)
	}
	srcInfo, err := os.Lstat(deep)
	must(t, err)
	dstInfo, err := os.Lstat(filepath.Join(target, "a", "b", "c", "deep.txt"))
	must(t, err)
	if srcInfo.Mode().Perm() != dstInfo.Mode().Perm() || !srcInfo.ModTime().Equal(dstInfo.ModTime()) {
		t.Fatalf("metadata mismatch: %o %v vs %o %v",
			srcInfo.Mode().Perm(), srcInfo.ModTime(), dstInfo.Mode().Perm(), dstInfo.ModTime())
	}
	reports, err := e.SelectiveRestoreFileReports(job.ID)
	must(t, err)
	if len(reports) != 4 { // three parents + one file; root implicit
		t.Fatalf("reports=%d want 4: %+v", len(reports), reports)
	}
	if reports[3].Status != repo.RestoreEntryVerified || len(reports[3].FileDigest) == 0 {
		t.Fatalf("file report not verified: %+v", reports[3])
	}

	// Same idempotent request must return the same job and never create a
	// second tree. Because the first target now exists, a fresh job would have
	// conflicted.
	again, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target,
		Paths: []string{"a/b/c/deep.txt"}, IdempotencyKey: "deep",
	})
	must(t, err)
	if again.ID != job.ID {
		t.Fatalf("idempotent job id=%d, want %d", again.ID, job.ID)
	}
	if _, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target,
		Paths: []string{"a/b/c/deep.txt"}, IdempotencyKey: "different",
	}); !errors.Is(err, backup.ErrTargetExists) {
		t.Fatalf("existing target conflict, got %v", err)
	}
	if _, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target,
		Paths: []string{"sibling.txt"}, IdempotencyKey: "deep",
	}); err == nil || !strings.Contains(err.Error(), "idempotency_key_conflict") {
		t.Fatalf("same key with different paths must conflict, got %v", err)
	}
}

func TestSelectiveRestoreRejectsMissingSymlinkDependencyBeforeTargetCreation(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(filepath.Join(src, "sub"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "sub", "deep.txt"), []byte("target\n"), 0o644))
	must(t, os.Symlink("sub/deep.txt", filepath.Join(src, "link")))
	snap, err := e.CreateSnapshot(src, "links", true)
	must(t, err)

	target := filepath.Join(dir, "out")
	_, _, err = e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target, Paths: []string{"link"},
	})
	var sel *backup.SelectiveRestoreError
	if !errors.As(err, &sel) {
		t.Fatalf("want selective rejection, got %v", err)
	}
	if sel.Code != "missing_symlink_dependency" {
		t.Fatalf("code=%s reasons=%v", sel.Code, sel.Reasons)
	}
	if !strings.Contains(sel.Error(), "sub/deep.txt") {
		t.Fatalf("dependency not named: %v", sel.Reasons)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target must not exist after preflight rejection, stat=%v", statErr)
	}
	left, err := filepath.Glob(filepath.Join(filepath.Dir(target), ".increstore-job-*"))
	must(t, err)
	if len(left) > 0 {
		t.Fatalf("no private staging may remain: %v", left)
	}

	// Including the target (which also pulls in its parent directory) is the
	// explicit, safe way to satisfy the dependency.
	job, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target,
		Paths: []string{"link", "sub/deep.txt"},
	})
	must(t, err)
	if got := waitRestoreJob(t, e, job.ID); got.Status != repo.RestoreStatusCompleted {
		t.Fatalf("status=%s %s %s", got.Status, got.Stage, got.Message)
	}
	targetString, err := os.Readlink(filepath.Join(target, "link"))
	must(t, err)
	if targetString != "sub/deep.txt" {
		t.Fatalf("link target rewritten to %q", targetString)
	}
}

func TestSelectiveRestoreCorruptChunkRetryDoesNotAffectFullRestore(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "large.txt"),
		[]byte(strings.Repeat("corrupt-me\n", 5000)), 0o644))
	snap, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)

	entries, err := e.Manifest.EntriesOf(snap.SnapshotID)
	must(t, err)
	var digest []byte
	for _, en := range entries {
		if en.RelPath == "large.txt" && len(en.ChunkDigests) > 0 {
			digest = append([]byte(nil), en.ChunkDigests[0]...)
		}
	}
	if digest == nil {
		t.Fatal("test file produced no chunks")
	}
	blob, err := e.Store.Path(digest)
	must(t, err)
	original, err := os.ReadFile(blob)
	must(t, err)

	target := filepath.Join(dir, "selective-corrupt")
	aboutToOpen := make(chan struct{})
	allowOpen := make(chan struct{})
	once := make(chan struct{})
	e.Fail.BeforeRestoreOpenChunk = func(_ int64, _ string, _ []byte) {
		select {
		case <-once:
		default:
			close(once)
			aboutToOpen <- struct{}{}
			<-allowOpen
		}
	}
	job, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target,
		Paths: []string{"large.txt"}, IdempotencyKey: "corrupt",
	})
	must(t, err)
	<-aboutToOpen
	e.Fail.BeforeRestoreOpenChunk = nil
	must(t, os.Chmod(blob, 0o644))
	must(t, os.WriteFile(blob, []byte("broken chunk contents"), 0o644))
	close(allowOpen)
	finished := waitRestoreJob(t, e, job.ID)
	if finished.Status != repo.RestoreStatusFailed {
		t.Fatalf("corruption should fail job, got %+v", finished)
	}
	if finished.RelPath != "large.txt" || finished.Stage != backup.RestoreStageEntries ||
		!strings.Contains(finished.Message, hex.EncodeToString(digest)) {
		t.Fatalf("failure cannot locate file/chunk: stage=%s rel=%s msg=%s",
			finished.Stage, finished.RelPath, finished.Message)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("failed job left published target: %v", statErr)
	}
	for _, ent := range waitFileReports(t, e, job.ID) {
		if ent.RelPath == "large.txt" && ent.Status != repo.RestoreEntryFailed {
			t.Fatalf("entry status=%s", ent.Status)
		}
	}

	// Simulate replacing the bad disk block with its verified content, then
	// retry the durable job.
	must(t, os.WriteFile(blob, original, 0o444))
	retried, err := e.RetrySelectiveRestore(job.ID)
	must(t, err)
	if retried.ID != job.ID {
		t.Fatalf("retry created job %d instead of reusing %d", retried.ID, job.ID)
	}
	if got := waitRestoreJob(t, e, job.ID); got.Status != repo.RestoreStatusCompleted {
		t.Fatalf("retry status=%s %s %s", got.Status, got.Stage, got.Message)
	}

	// Other committed snapshots still support the existing whole-tree API.
	full, err := e.Restore(snap.SnapshotID, filepath.Join(dir, "full"))
	must(t, err)
	if full.Files != 1 {
		t.Fatalf("full restore files=%d", full.Files)
	}
}

func waitFileReports(t *testing.T, e *backup.Engine, id int64) []repo.RestoreJobEntry {
	t.Helper()
	reports, err := e.SelectiveRestoreFileReports(id)
	if err != nil {
		t.Fatal(err)
	}
	return reports
}

func TestSelectiveRestoreCrashRecoveryContinuesOnlyOwnedStaging(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "f.txt"), []byte("crash\n"), 0o644))

	open := func() *backup.Engine {
		m, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		s, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
		if err != nil {
			t.Fatal(err)
		}
		e, err := backup.NewEngine(m, s)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	snap, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	target := filepath.Join(dir, "recovered")
	job, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
		SnapshotID: snap.SnapshotID, Target: target, Paths: []string{"f.txt"},
	})
	must(t, err)
	finished := waitRestoreJob(t, e, job.ID)
	if finished.Status != repo.RestoreStatusCompleted {
		t.Fatalf("setup job: %+v", finished)
	}
	must(t, os.RemoveAll(target))
	staging := filepath.Join(filepath.Dir(target), ".simulated-other")
	must(t, os.Mkdir(staging, 0o700))
	must(t, os.WriteFile(filepath.Join(staging, "foreign"), []byte("x"), 0o600))
	must(t, e.Manifest.SetRestoreJobStaging(job.ID, filepath.Join(filepath.Dir(target), ".increstore-job-owned")))
	must(t, os.Mkdir(filepath.Join(filepath.Dir(target), ".increstore-job-owned"), 0o700))
	if _, err := e.Manifest.DB().Exec(`UPDATE restore_jobs SET status='running' WHERE id=?`, job.ID); err != nil {
		t.Fatal(err)
	}
	e.Manifest.Close()

	e2 := open()
	must(t, e2.RecoverSelectiveRestores())
	got := waitRestoreJob(t, e2, job.ID)
	if got.Status != repo.RestoreStatusCompleted {
		t.Fatalf("recovered job status=%s msg=%s", got.Status, got.Message)
	}
	if _, err := os.Lstat(target); err != nil {
		t.Fatalf("target not recovered: %v", err)
	}
	if _, err := os.Lstat(staging); err != nil {
		t.Fatalf("recovery removed staging it did not own: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(target), ".increstore-job-owned")); !os.IsNotExist(err) {
		t.Fatalf("owned staging not cleaned: %v", err)
	}
}

func TestSelectiveRestoreHTTPAPI(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "api.txt"), []byte("api\n"), 0o644))
	snap, err := e.CreateSnapshot(src, "api", true)
	must(t, err)

	srv := httptest.NewServer((&api.Server{Engine: e}).NewRouter())
	defer srv.Close()
	body := strings.NewReader(`{"snapshot_id":` + fmt.Sprint(snap.SnapshotID) +
		`,"target":"` + filepath.Join(dir, "api-out") +
		`","paths":["api.txt"],"idempotency_key":"http"}`)
	resp, err := http.Post(srv.URL+"/v1/selective-restores", "application/json", body)
	must(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d", resp.StatusCode)
	}
	var created struct {
		ID int64 `json:"id"`
	}
	must(t, json.NewDecoder(resp.Body).Decode(&created))
	finished := waitRestoreJob(t, e, created.ID)
	if finished.Status != repo.RestoreStatusCompleted {
		t.Fatalf("job=%+v", finished)
	}
	filesResp, err := http.Get(srv.URL + fmt.Sprintf("/v1/selective-restores/%d/files", created.ID))
	must(t, err)
	defer filesResp.Body.Close()
	if filesResp.StatusCode != http.StatusOK {
		t.Fatalf("files status=%d", filesResp.StatusCode)
	}
	var report struct {
		Files []struct {
			Status string `json:"status"`
			Digest string `json:"digest"`
		} `json:"files"`
	}
	must(t, json.NewDecoder(filesResp.Body).Decode(&report))
	if len(report.Files) != 1 || report.Files[0].Status != repo.RestoreEntryVerified ||
		report.Files[0].Digest == "" {
		t.Fatalf("bad report: %+v", report)
	}
}

func TestSelectiveRestoreNormalizesRequestedPaths(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(filepath.Join(src, "d"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "d", "f"), []byte("x"), 0o644))
	snap, err := e.CreateSnapshot(src, "v", true)
	must(t, err)
	for _, bad := range []string{"/d/f", "../outside", "d/../../x"} {
		_, _, err := e.CreateSelectiveRestore(backup.SelectiveRestoreRequest{
			SnapshotID: snap.SnapshotID, Target: filepath.Join(dir, "bad-target"),
			Paths: []string{bad},
		})
		if err == nil {
			t.Fatalf("bad path %q accepted", bad)
		}
	}
}
