// Package api exposes the backup engine over a small local HTTP API.
package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// Server wires the engine to HTTP.
type Server struct {
	Engine *backup.Engine
}

// NewRouter builds the mux.
func (s *Server) NewRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/recover", s.recover)
	mux.HandleFunc("GET /v1/snapshots", s.list)
	mux.HandleFunc("POST /v1/snapshots", s.create)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.get)
	mux.HandleFunc("POST /v1/snapshots/{id}/verify", s.verify)
	mux.HandleFunc("GET /v1/snapshots/{id}/errors", s.listErrors)
	mux.HandleFunc("GET /v1/snapshots/{id}/missing", s.missing)
	mux.HandleFunc("POST /v1/snapshots/{id}/restore", s.restore)
	mux.HandleFunc("POST /v1/restore-jobs", s.createRestoreJob)
	mux.HandleFunc("GET /v1/restore-jobs", s.listRestoreJobs)
	mux.HandleFunc("GET /v1/restore-jobs/{id}", s.getRestoreJob)
	mux.HandleFunc("POST /v1/restore-jobs/{id}/retry", s.retryRestoreJob)
	mux.HandleFunc("GET /v1/restore-jobs/{id}/files", s.restoreJobFiles)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string, details any) {
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": msg,
		"details": details,
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

type snapshotResp struct {
	ID          int64      `json:"id"`
	RootPath    string     `json:"root_path"`
	Status      string     `json:"status"`
	FileCount   int64      `json:"file_count"`
	DirCount    int64      `json:"dir_count"`
	BytesTotal  int64      `json:"bytes_total"`
	ChunksNew   int64      `json:"chunks_new"`
	ChunksRef   int64      `json:"chunks_referenced"`
	Polynomial  string     `json:"polynomial"`
	CreatedAt   time.Time  `json:"created_at"`
	CommittedAt *time.Time `json:"committed_at,omitempty"`
	Message     string     `json:"message"`
}

func toSnapshotResp(si repo.SnapshotInfo) snapshotResp {
	return snapshotResp{
		ID:          si.ID,
		RootPath:    si.RootPath,
		Status:      si.Status,
		FileCount:   si.FileCount,
		DirCount:    si.DirCount,
		BytesTotal:  si.BytesTotal,
		ChunksNew:   si.ChunksNew,
		ChunksRef:   si.ChunksRef,
		Polynomial:  "0x" + strconv.FormatUint(si.Polynomial, 16),
		CreatedAt:   si.CreatedAt,
		CommittedAt: si.CommittedAt,
		Message:     si.Message,
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	all, err := s.Engine.Manifest.ListSnapshots()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]snapshotResp, 0, len(all))
	for _, si := range all {
		out = append(out, toSnapshotResp(si))
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": out})
}

type createReq struct {
	Root          string `json:"root"`
	Message       string `json:"message"`
	Finish        *bool  `json:"finish"`      // default true; false = die before commit (demo)
	LoseChunks    int    `json:"lose_chunks"` // failpoint: delete N blobs pre-verify
	UnstableRetry int    `json:"-"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
	}
	if strings.TrimSpace(req.Root) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "root is required", nil)
		return
	}
	finish := true
	if req.Finish != nil {
		finish = *req.Finish
	}
	s.Engine.Fail.LoseChunkCount = req.LoseChunks
	defer func() { s.Engine.Fail.LoseChunkCount = 0 }()

	res, err := s.Engine.CreateSnapshot(req.Root, req.Message, finish)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": rej.SnapshotID,
				"status":      repo.StatusFailed,
				"error":       "snapshot_rejected",
				"reasons":     rej.Reasons,
				"hint":        "GET /v1/snapshots/" + strconv.FormatInt(rej.SnapshotID, 10) + "/missing",
			})
			return
		}
		status := http.StatusInternalServerError
		if res == nil {
			writeErr(w, status, "snapshot_failed", err.Error(), nil)
			return
		}
		writeJSON(w, status, map[string]any{
			"snapshot_id": res.SnapshotID,
			"status":      res.Status,
			"error":       err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id":       res.SnapshotID,
		"status":            res.Status,
		"chunks_new":        res.NewChunks,
		"chunks_referenced": res.RefChunks,
	})
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", "snapshot id must be an integer", nil)
		return 0, false
	}
	return id, true
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, toSnapshotResp(si))
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	res, err := s.Engine.VerifyAndFinalize(id)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": id,
				"status":      repo.StatusFailed,
				"error":       "verification_failed",
				"missing":     rej.Reasons,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, "verify_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot_id": id,
		"status":      res.Status,
	})
}

type missingResp struct {
	SnapshotID int64             `json:"snapshot_id"`
	Status     string            `json:"status"`
	Missing    []missingItemResp `json:"missing"`
}

type missingItemResp struct {
	RelPath  string `json:"rel_path"`
	Digest   string `json:"chunk_digest"`
	Length   int64  `json:"declared_length"`
	BlobPath string `json:"expected_blob_path"`
	Reason   string `json:"reason"`
}

func (s *Server) missing(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	found, err := s.Engine.Manifest.FindMissingChunks(id, func(digest []byte, length int64) (bool, error) {
		return s.Engine.Store.Has(digest, length)
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	items := make([]missingItemResp, 0, len(found))
	for _, mc := range found {
		item := missingItemResp{
			RelPath: mc.RelPath,
			Digest:  hex.EncodeToString(mc.Digest),
			Length:  mc.Length,
			Reason:  mc.Reason,
		}
		if p, err := s.Engine.Store.Path(mc.Digest); err == nil {
			item.BlobPath = p
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, missingResp{SnapshotID: id, Status: si.Status, Missing: items})
}

func (s *Server) listErrors(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	errs, err := s.Engine.Manifest.ListErrors(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type item struct {
		Stage       string    `json:"stage"`
		RelPath     string    `json:"rel_path"`
		ChunkDigest string    `json:"chunk_digest,omitempty"`
		Message     string    `json:"message"`
		CreatedAt   time.Time `json:"created_at"`
	}
	out := make([]item, 0, len(errs))
	for _, e := range errs {
		it := item{Stage: e.Stage, RelPath: e.RelPath, Message: e.Message, CreatedAt: e.CreatedAt}
		if len(e.ChunkDigest) > 0 {
			it.ChunkDigest = hex.EncodeToString(e.ChunkDigest)
		}
		out = append(out, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": id, "errors": out})
}

type restoreReq struct {
	Target string `json:"target"`
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req restoreReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	if strings.TrimSpace(req.Target) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "target is required", nil)
		return
	}
	res, err := s.Engine.Restore(id, req.Target)
	if err != nil {
		if errors.Is(err, backup.ErrTargetExists) {
			writeErr(w, http.StatusConflict, "target_exists", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "only committed snapshots") {
			writeErr(w, http.StatusConflict, "snapshot_not_committed", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "escapes restore root") {
			writeErr(w, http.StatusUnprocessableEntity, "unsafe_symlink", err.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "restore_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id": res.SnapshotID,
		"target":      res.Target,
		"files":       res.Files,
		"directories": res.Dirs,
		"symlinks":    res.Symlinks,
		"bytes":       res.Bytes,
		"verified":    res.Verified,
	})
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	out, err := s.Engine.RecoverPending()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "recover_failed", err.Error(), nil)
		return
	}
	ids := make([]map[string]any, 0, len(out))
	for _, r := range out {
		ids = append(ids, map[string]any{"snapshot_id": r.SnapshotID, "status": r.Status})
	}
	writeJSON(w, http.StatusOK, map[string]any{"recovered": ids})
}

// ---------- selective restore jobs ----------

type restoreJobResp struct {
	ID             int64      `json:"id"`
	IdempotencyKey string     `json:"idempotency_key"`
	SnapshotID     int64      `json:"snapshot_id"`
	Target         string     `json:"target"`
	Status         string     `json:"status"`
	Paths          []string   `json:"paths"`
	Error          string     `json:"error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
}

func toRestoreJobResp(j repo.RestoreJob) restoreJobResp {
	return restoreJobResp{
		ID:             j.ID,
		IdempotencyKey: j.IdempotencyKey,
		SnapshotID:     j.SnapshotID,
		Target:         j.Target,
		Status:         j.Status,
		Paths:          j.RequestedPaths,
		Error:          j.Error,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		FinishedAt:     j.FinishedAt,
	}
}

type restoreJobFileResp struct {
	RelPath    string `json:"rel_path"`
	Kind       string `json:"kind"`
	Implicit   bool   `json:"implicit"` // true = auto-added parent directory
	Status     string `json:"status"`
	Size       int64  `json:"size"`
	Digest     string `json:"digest,omitempty"`
	ChunkCount int    `json:"chunk_count"`
	Error      string `json:"error,omitempty"`
}

func toRestoreJobFileResps(files []repo.RestoreJobFile) []restoreJobFileResp {
	out := make([]restoreJobFileResp, 0, len(files))
	for _, f := range files {
		out = append(out, restoreJobFileResp{
			RelPath:    f.RelPath,
			Kind:       f.Kind,
			Implicit:   f.Implicit,
			Status:     f.Status,
			Size:       f.Size,
			Digest:     f.Digest,
			ChunkCount: f.ChunkCount,
			Error:      f.Error,
		})
	}
	return out
}

func writeRestoreJob(w http.ResponseWriter, status int, res *backup.RestoreJobResult, replayed bool) {
	writeJSON(w, status, map[string]any{
		"job":               toRestoreJobResp(res.Job),
		"files":             toRestoreJobFileResps(res.Files),
		"idempotent_replay": replayed,
	})
}

// writeRestoreJobError maps engine errors to statuses. When a job row exists
// (precheck passed, execution failed) the failed job and its per-file report
// are the response — that is what maintenance retries.
func (s *Server) writeRestoreJobError(w http.ResponseWriter, res *backup.RestoreJobResult, err error) {
	if res != nil {
		code := "restore_job_failed"
		if errors.Is(err, backup.ErrTargetExists) {
			code = "target_exists"
		}
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":   code,
			"message": err.Error(),
			"job":     toRestoreJobResp(res.Job),
			"files":   toRestoreJobFileResps(res.Files),
		})
		return
	}
	var unknown *backup.ErrUnknownPath
	var deps *backup.ErrMissingLinkDeps
	var idem *backup.ErrIdempotencyConflict
	switch {
	case errors.As(err, &unknown):
		writeErr(w, http.StatusBadRequest, "unknown_path", err.Error(), map[string]string{"path": unknown.Path})
	case errors.As(err, &deps):
		writeErr(w, http.StatusUnprocessableEntity, "missing_link_deps", err.Error(),
			map[string]any{"missing": deps.Paths})
	case errors.As(err, &idem):
		writeErr(w, http.StatusConflict, "idempotency_key_conflict", err.Error(), nil)
	case errors.Is(err, backup.ErrTargetExists):
		writeErr(w, http.StatusConflict, "target_exists", err.Error(), nil)
	case errors.Is(err, repo.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
	case strings.Contains(err.Error(), "only committed snapshots"):
		writeErr(w, http.StatusConflict, "snapshot_not_committed", err.Error(), nil)
	case strings.Contains(err.Error(), "escapes restore root"):
		writeErr(w, http.StatusUnprocessableEntity, "unsafe_symlink", err.Error(), nil)
	case strings.Contains(err.Error(), "symlink ancestor"):
		writeErr(w, http.StatusUnprocessableEntity, "unsafe_ancestor", err.Error(), nil)
	default:
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
	}
}

type createRestoreJobReq struct {
	SnapshotID     int64    `json:"snapshot_id"`
	Target         string   `json:"target"`
	Paths          []string `json:"paths"`
	IdempotencyKey string   `json:"idempotency_key"`
}

func (s *Server) createRestoreJob(w http.ResponseWriter, r *http.Request) {
	var req createRestoreJobReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	res, err := s.Engine.CreateRestoreJob(backup.SelectiveRestoreRequest{
		SnapshotID:     req.SnapshotID,
		Target:         req.Target,
		Paths:          req.Paths,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		s.writeRestoreJobError(w, res, err)
		return
	}
	if res.Replayed {
		writeRestoreJob(w, http.StatusOK, res, true)
		return
	}
	writeRestoreJob(w, http.StatusCreated, res, false)
}

func (s *Server) listRestoreJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.Engine.Manifest.ListRestoreJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]restoreJobResp, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toRestoreJobResp(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"restore_jobs": out})
}

func (s *Server) getRestoreJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := s.Engine.Manifest.GetRestoreJob(id)
	if errors.Is(err, repo.ErrJobNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "restore job does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	files, err := s.Engine.Manifest.RestoreJobFiles(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job":   toRestoreJobResp(job),
		"files": toRestoreJobFileResps(files),
	})
}

func (s *Server) retryRestoreJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	res, err := s.Engine.RetryRestoreJob(id)
	if err != nil {
		if errors.Is(err, repo.ErrJobNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "restore job does not exist", nil)
			return
		}
		if strings.Contains(err.Error(), "is running") {
			writeErr(w, http.StatusConflict, "job_running", err.Error(), nil)
			return
		}
		s.writeRestoreJobError(w, res, err)
		return
	}
	writeRestoreJob(w, http.StatusOK, res, false)
}

func (s *Server) restoreJobFiles(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if _, err := s.Engine.Manifest.GetRestoreJob(id); errors.Is(err, repo.ErrJobNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "restore job does not exist", nil)
		return
	}
	files, err := s.Engine.Manifest.RestoreJobFiles(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": id,
		"files":  toRestoreJobFileResps(files),
	})
}
