package repo

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// RestoreJob is one frozen selective-restore request and its lifecycle.
type RestoreJob struct {
	ID             int64
	IdempotencyKey string
	SnapshotID     int64
	Target         string
	Staging        string
	Status         string
	RequestedPaths []string // normalized, sorted, deduplicated; frozen at creation
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}

// RestoreJobFile is the per-file plan/report row of a restore job.
type RestoreJobFile struct {
	JobID      int64
	RelPath    string
	Kind       string
	Implicit   bool // auto-added parent directory, not explicitly requested
	Status     string
	Size       int64
	Digest     string
	ChunkCount int
	Error      string
}

// ErrJobNotFound marks a missing restore job.
var ErrJobNotFound = errors.New("restore job not found")

const jobCols = `id, idempotency_key, snapshot_id, target, staging, status,
	requested_paths, error, created_at, updated_at, finished_at`

func scanJob(row interface{ Scan(...any) error }) (RestoreJob, error) {
	var j RestoreJob
	var paths string
	var created, updated string
	var finished sql.NullString
	if err := row.Scan(&j.ID, &j.IdempotencyKey, &j.SnapshotID, &j.Target,
		&j.Staging, &j.Status, &paths, &j.Error, &created, &updated, &finished); err != nil {
		return j, err
	}
	if err := json.Unmarshal([]byte(paths), &j.RequestedPaths); err != nil {
		return j, fmt.Errorf("job %d: corrupt requested_paths: %w", j.ID, err)
	}
	j.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	if finished.Valid {
		t, err := time.Parse(time.RFC3339Nano, finished.String)
		if err == nil {
			j.FinishedAt = &t
		}
	}
	return j, nil
}

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// InsertRestoreJob persists a new pending job. The idempotency key is unique;
// callers check GetRestoreJobByKey first and treat a constraint violation as
// a lost race, re-reading the winner's row.
func (m *Manifest) InsertRestoreJob(j *RestoreJob) (int64, error) {
	paths, err := json.Marshal(j.RequestedPaths)
	if err != nil {
		return 0, err
	}
	res, err := m.db.Exec(`INSERT INTO restore_jobs
		(idempotency_key, snapshot_id, target, staging, status, requested_paths, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?)`,
		j.IdempotencyKey, j.SnapshotID, j.Target, j.Staging, JobPending,
		string(paths), nowUTC(), nowUTC())
	if err != nil {
		return 0, fmt.Errorf("insert restore job: %w", err)
	}
	return res.LastInsertId()
}

// GetRestoreJob fetches one job by id.
func (m *Manifest) GetRestoreJob(id int64) (RestoreJob, error) {
	j, err := scanJob(m.db.QueryRow(`SELECT `+jobCols+` FROM restore_jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrJobNotFound
	}
	return j, err
}

// GetRestoreJobByKey fetches one job by idempotency key.
func (m *Manifest) GetRestoreJobByKey(key string) (RestoreJob, error) {
	j, err := scanJob(m.db.QueryRow(`SELECT `+jobCols+` FROM restore_jobs WHERE idempotency_key = ?`, key))
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrJobNotFound
	}
	return j, err
}

// ListRestoreJobs returns all jobs, newest first.
func (m *Manifest) ListRestoreJobs() ([]RestoreJob, error) {
	rows, err := m.db.Query(`SELECT ` + jobCols + ` FROM restore_jobs ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RestoreJob
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// RestoreJobsWithStatus returns jobs in one of the given states, oldest first.
func (m *Manifest) RestoreJobsWithStatus(statuses ...string) ([]RestoreJob, error) {
	q := `SELECT ` + jobCols + ` FROM restore_jobs WHERE status IN (`
	args := make([]any, 0, len(statuses))
	for i, s := range statuses {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, s)
	}
	q += `) ORDER BY id`
	rows, err := m.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RestoreJob
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// UpdateRestoreJobStatus moves a job to a new state. Terminal states
// (done/failed) stamp finished_at.
func (m *Manifest) UpdateRestoreJobStatus(id int64, status, errMsg string) error {
	finished := status == JobDone || status == JobFailed
	var fin any
	if finished {
		fin = nowUTC()
	}
	res, err := m.db.Exec(`UPDATE restore_jobs
		SET status = ?, error = ?, updated_at = ?, finished_at = COALESCE(?, finished_at)
		WHERE id = ?`, status, errMsg, nowUTC(), fin, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrJobNotFound
	}
	return nil
}

// ReplaceRestoreJobFiles swaps the whole plan of a job (used when a retry
// resets every file back to pending).
func (m *Manifest) ReplaceRestoreJobFiles(jobID int64, files []RestoreJobFile) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM restore_job_files WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	for _, f := range files {
		implicit := 0
		if f.Implicit {
			implicit = 1
		}
		if _, err := tx.Exec(`INSERT INTO restore_job_files
			(job_id, rel_path, kind, implicit, status) VALUES (?,?,?,?,?)`,
			jobID, f.RelPath, f.Kind, implicit, JobPending); err != nil {
			return fmt.Errorf("plan %q: %w", f.RelPath, err)
		}
	}
	return tx.Commit()
}

// MarkRestoreJobFileDone records the verified measurements of one restored file.
func (m *Manifest) MarkRestoreJobFileDone(jobID int64, rel string, size int64, digest string, chunks int) error {
	_, err := m.db.Exec(`UPDATE restore_job_files
		SET status = ?, size = ?, digest = ?, chunk_count = ?, error = ''
		WHERE job_id = ? AND rel_path = ?`,
		JobDone, size, digest, chunks, jobID, rel)
	return err
}

// MarkRestoreJobFileFailed records why one path could not be restored.
func (m *Manifest) MarkRestoreJobFileFailed(jobID int64, rel, errMsg string) error {
	_, err := m.db.Exec(`UPDATE restore_job_files
		SET status = ?, error = ? WHERE job_id = ? AND rel_path = ?`,
		JobFailed, errMsg, jobID, rel)
	return err
}

// RestoreJobFiles returns the per-file plan/report of a job, ordered by path.
func (m *Manifest) RestoreJobFiles(jobID int64) ([]RestoreJobFile, error) {
	rows, err := m.db.Query(`SELECT rel_path, kind, implicit, status, size, digest, chunk_count, error
		FROM restore_job_files WHERE job_id = ? ORDER BY rel_path`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RestoreJobFile
	for rows.Next() {
		var f RestoreJobFile
		var implicit int
		if err := rows.Scan(&f.RelPath, &f.Kind, &implicit, &f.Status,
			&f.Size, &f.Digest, &f.ChunkCount, &f.Error); err != nil {
			return nil, err
		}
		f.JobID = jobID
		f.Implicit = implicit != 0
		out = append(out, f)
	}
	return out, rows.Err()
}
