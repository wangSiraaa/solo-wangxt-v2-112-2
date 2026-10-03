package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	RestoreStatusPending   = "pending"
	RestoreStatusRunning   = "running"
	RestoreStatusFailed    = "failed"
	RestoreStatusCompleted = "completed"

	RestorePathRequested = "requested"
	RestorePathEffective = "effective"

	RestoreEntryPending  = "pending"
	RestoreEntryRunning  = "running"
	RestoreEntryFailed   = "failed"
	RestoreEntryVerified = "verified"
)

// ErrRestoreJobNotFound marks a missing selective restore job.
var ErrRestoreJobNotFound = errors.New("restore job not found")

// RestoreJobInfo is the durable state of one selective restore.
type RestoreJobInfo struct {
	ID           int64
	SnapshotID   int64
	Target       string
	Idempotency  string
	Status       string
	StagingPath  string
	Stage        string
	RelPath      string
	Message      string
	FileCount    int64
	DirCount     int64
	SymlinkCount int64
	BytesTotal   int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time
}

// RestoreJobEntry is the persisted per-entry status and verification report.
type RestoreJobEntry struct {
	RelPath    string
	Kind       string
	EntryOrder int
	Status     string
	Size       int64
	FileDigest []byte
	Mode       uint32
	ModTime    time.Time
	ChunkCount int
	LinkTarget string
	Error      string
	UpdatedAt  time.Time
}

// RestoreJobCreate is the immutable request frozen by CreateRestoreJob.
type RestoreJobCreate struct {
	SnapshotID  int64
	Target      string
	Idempotency string
	Paths       []string
	Entries     []RestoreJobEntry
	StagingName string
}

const restoreJobCols = `id, snapshot_id, target_path, COALESCE(idempotency_key, ''),
	status, staging_path, stage, rel_path, message, file_count, dir_count,
	symlink_count, bytes_total, created_at, updated_at, started_at, finished_at`

func scanRestoreJob(row interface{ Scan(...any) error }) (RestoreJobInfo, error) {
	var j RestoreJobInfo
	var created, updated string
	var started, finished sql.NullString
	err := row.Scan(&j.ID, &j.SnapshotID, &j.Target, &j.Idempotency,
		&j.Status, &j.StagingPath, &j.Stage, &j.RelPath, &j.Message,
		&j.FileCount, &j.DirCount, &j.SymlinkCount, &j.BytesTotal,
		&created, &updated, &started, &finished)
	if err != nil {
		return j, err
	}
	j.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	if started.Valid {
		t, _ := time.Parse(time.RFC3339Nano, started.String)
		j.StartedAt = &t
	}
	if finished.Valid {
		t, _ := time.Parse(time.RFC3339Nano, finished.String)
		j.FinishedAt = &t
	}
	return j, nil
}

// CreateRestoreJob inserts the job, its frozen paths, and per-entry reports in
// one transaction.
func (m *Manifest) CreateRestoreJob(in RestoreJobCreate) (RestoreJobInfo, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return RestoreJobInfo{}, err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(`INSERT INTO restore_jobs
		(snapshot_id, target_path, idempotency_key, status, staging_path,
		 created_at, updated_at)
		VALUES (?,?,?,?,?,?,?)`,
		in.SnapshotID, in.Target, nullableText(in.Idempotency), RestoreStatusPending,
		"", now, now)
	if err != nil {
		return RestoreJobInfo{}, fmt.Errorf("create restore job: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return RestoreJobInfo{}, err
	}
	var staging string
	if in.StagingName == "" {
		staging = fmt.Sprintf(".increstore-job-%d", id)
	} else {
		staging = fmt.Sprintf(".increstore-job-%d-%s", id, in.StagingName)
	}
	if _, err := tx.Exec(`UPDATE restore_jobs SET staging_path = ? WHERE id = ?`,
		staging, id); err != nil {
		return RestoreJobInfo{}, err
	}

	seen := map[string]bool{}
	for _, p := range in.Paths {
		if seen[p] {
			continue
		}
		seen[p] = true
		if _, err := tx.Exec(`INSERT INTO restore_job_paths
			(job_id, rel_path, relation) VALUES (?,?,?)`,
			id, p, RestorePathRequested); err != nil {
			return RestoreJobInfo{}, err
		}
	}
	for _, en := range in.Entries {
		if err := insertRestoreJobEntry(tx, id, now, en); err != nil {
			return RestoreJobInfo{}, err
		}
		if _, err := tx.Exec(`INSERT INTO restore_job_paths
			(job_id, rel_path, relation, entry_order) VALUES (?,?,?,?)`,
			id, en.RelPath, RestorePathEffective, en.EntryOrder); err != nil {
			return RestoreJobInfo{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return RestoreJobInfo{}, err
	}
	return m.GetRestoreJob(id)
}

func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func insertRestoreJobEntry(tx *sql.Tx, jobID int64, now string, en RestoreJobEntry) error {
	_, err := tx.Exec(`INSERT INTO restore_job_entries
		(job_id, rel_path, kind, entry_order, status, size, file_digest, mode,
		 mod_time_ns, chunk_count, link_target, error, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		jobID, en.RelPath, en.Kind, en.EntryOrder, RestoreEntryPending, en.Size,
		en.FileDigest, int64(en.Mode), en.ModTime.UnixNano(), en.ChunkCount,
		en.LinkTarget, en.Error, now)
	return err
}

// GetRestoreJob fetches one selective restore job.
func (m *Manifest) GetRestoreJob(id int64) (RestoreJobInfo, error) {
	row := m.db.QueryRow(`SELECT `+restoreJobCols+` FROM restore_jobs WHERE id = ?`, id)
	j, err := scanRestoreJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrRestoreJobNotFound
	}
	return j, err
}

// GetRestoreJobByIdempotencyKey returns a job previously created with the key.
func (m *Manifest) GetRestoreJobByIdempotencyKey(key string) (RestoreJobInfo, bool, error) {
	if key == "" {
		return RestoreJobInfo{}, false, nil
	}
	row := m.db.QueryRow(`SELECT `+restoreJobCols+` FROM restore_jobs
		WHERE idempotency_key = ?`, key)
	j, err := scanRestoreJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return RestoreJobInfo{}, false, nil
	}
	if err != nil {
		return RestoreJobInfo{}, false, err
	}
	return j, true, nil
}

// ListRestoreJobsByStatus returns jobs in the given status, oldest first. An
// empty status returns all jobs.
func (m *Manifest) ListRestoreJobsByStatus(status string) ([]RestoreJobInfo, error) {
	q := `SELECT ` + restoreJobCols + ` FROM restore_jobs`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY id`
	rows, err := m.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RestoreJobInfo
	for rows.Next() {
		j, err := scanRestoreJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// RestoreJobPaths returns frozen paths for one relation.
func (m *Manifest) RestoreJobPaths(jobID int64, relation string) ([]string, error) {
	rows, err := m.db.Query(`SELECT rel_path FROM restore_job_paths
		WHERE job_id = ? AND relation = ? ORDER BY rel_path`, jobID, relation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ListRestoreJobEntries returns per-file/per-entry reports in entry order.
func (m *Manifest) ListRestoreJobEntries(jobID int64) ([]RestoreJobEntry, error) {
	rows, err := m.db.Query(`SELECT rel_path, kind, entry_order, status, size,
		file_digest, mode, mod_time_ns, chunk_count, link_target, error, updated_at
		FROM restore_job_entries WHERE job_id = ? ORDER BY entry_order`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RestoreJobEntry
	for rows.Next() {
		var en RestoreJobEntry
		var ns int64
		var digest sql.NullString
		var updated string
		if err := rows.Scan(&en.RelPath, &en.Kind, &en.EntryOrder, &en.Status,
			&en.Size, &digest, &en.Mode, &ns, &en.ChunkCount,
			&en.LinkTarget, &en.Error, &updated); err != nil {
			return nil, err
		}
		if digest.Valid && len(digest.String) > 0 {
			en.FileDigest = []byte(digest.String)
		}
		en.ModTime = time.Unix(0, ns).UTC()
		en.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
		out = append(out, en)
	}
	return out, rows.Err()
}

// StartRestoreJob atomically marks a queued job as running.
func (m *Manifest) StartRestoreJob(id int64, staging string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := m.db.Exec(`UPDATE restore_jobs
		SET status = ?, staging_path = ?, stage = '', rel_path = '', message = '',
		    started_at = COALESCE(started_at, ?), updated_at = ?
		WHERE id = ? AND status = ?`,
		RestoreStatusRunning, staging, now, now, id, RestoreStatusPending)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("restore job %d is not pending", id)
	}
	return nil
}

// ResetFailedRestoreJob queues a failed job for another attempt and replaces
// its old per-entry reports with the current manifest plan.
func (m *Manifest) ResetFailedRestoreJob(id int64, staging string, entries []RestoreJobEntry) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(`UPDATE restore_jobs
		SET status = ?, staging_path = ?, stage = '', rel_path = '', message = '',
		    file_count = 0, dir_count = 0, symlink_count = 0, bytes_total = 0,
		    started_at = NULL, finished_at = NULL, updated_at = ?
		WHERE id = ? AND status = ?`,
		RestoreStatusPending, staging, now, id, RestoreStatusFailed)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("restore job %d is not failed", id)
	}
	if _, err := tx.Exec(`DELETE FROM restore_job_entries WHERE job_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM restore_job_paths
		WHERE job_id = ? AND relation = ?`, id, RestorePathEffective); err != nil {
		return err
	}
	for _, en := range entries {
		if err := insertRestoreJobEntry(tx, id, now, en); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO restore_job_paths
			(job_id, rel_path, relation, entry_order) VALUES (?,?,?,?)`,
			id, en.RelPath, RestorePathEffective, en.EntryOrder); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// FailRestoreJob records terminal failure location and message.
func (m *Manifest) FailRestoreJob(id int64, stage, rel, msg string) error {
	_, err := m.db.Exec(`UPDATE restore_jobs
		SET status = ?, stage = ?, rel_path = ?, message = ?,
		    finished_at = ?, updated_at = ?
		WHERE id = ?`,
		RestoreStatusFailed, stage, rel, msg,
		time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

// CompleteRestoreJob marks the job completed and stores aggregate counts.
func (m *Manifest) CompleteRestoreJob(id int64, files, dirs, symlinks int, bytes int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := m.db.Exec(`UPDATE restore_jobs
		SET status = ?, stage = 'complete', rel_path = '', message = '',
		    file_count = ?, dir_count = ?, symlink_count = ?, bytes_total = ?,
		    finished_at = ?, updated_at = ?
		WHERE id = ?`,
		RestoreStatusCompleted, files, dirs, symlinks, bytes, now, now, id)
	return err
}

// UpdateRestoreJobEntry stores one entry's latest state and report.
func (m *Manifest) UpdateRestoreJobEntry(jobID int64, en RestoreJobEntry) error {
	_, err := m.db.Exec(`UPDATE restore_job_entries
		SET status = ?, size = ?, file_digest = ?, chunk_count = ?,
		    link_target = ?, error = ?, updated_at = ?
		WHERE job_id = ? AND rel_path = ?`,
		en.Status, en.Size, en.FileDigest, en.ChunkCount, en.LinkTarget, en.Error,
		time.Now().UTC().Format(time.RFC3339Nano), jobID, en.RelPath)
	return err
}

// QueueInterruptedRestoreJob returns a running job left by a dead process to
// pending. It is called only during startup, after this process has checked
// the previously published target and owned staging directory.
func (m *Manifest) QueueInterruptedRestoreJob(id int64) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(`UPDATE restore_jobs
		SET status = ?, stage = 'recover', message = '', finished_at = NULL,
		    updated_at = ?
		WHERE id = ? AND status = ?`,
		RestoreStatusPending, now, id, RestoreStatusRunning)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("restore job %d is not running", id)
	}
	if _, err := tx.Exec(`UPDATE restore_job_entries
		SET status = ?, error = '', updated_at = ?
		WHERE job_id = ? AND status <> ?`,
		RestoreEntryPending, now, id, RestoreEntryVerified); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkPublishedRestoreJob is used by crash reconciliation when the rename is
// already visible but the process died before the status was committed.
func (m *Manifest) MarkPublishedRestoreJob(id int64, files, dirs, symlinks int, bytes int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := m.db.Exec(`UPDATE restore_jobs
		SET status = ?, staging_path = '', stage = 'complete', rel_path = '',
		    message = '', file_count = ?, dir_count = ?, symlink_count = ?,
		    bytes_total = ?, finished_at = COALESCE(finished_at, ?), updated_at = ?
		WHERE id = ?`,
		RestoreStatusCompleted, files, dirs, symlinks, bytes, now, now, id)
	return err
}

// MarkRestoreJobEntryFailed records a per-entry failure without erasing its
// frozen digest/metadata report.
func (m *Manifest) MarkRestoreJobEntryFailed(jobID int64, rel, msg string) error {
	if rel == "" {
		return nil
	}
	_, err := m.db.Exec(`UPDATE restore_job_entries
		SET status = ?, error = ?, updated_at = ?
		WHERE job_id = ? AND rel_path = ?`,
		RestoreEntryFailed, msg, time.Now().UTC().Format(time.RFC3339Nano), jobID, rel)
	return err
}

// SetRestoreJobStaging records where the private staging tree exists.
func (m *Manifest) SetRestoreJobStaging(id int64, staging string) error {
	_, err := m.db.Exec(`UPDATE restore_jobs SET staging_path = ?, updated_at = ?
		WHERE id = ?`, staging, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}
