// Command demo drives the backup HTTP API through the full failure story:
//
//  1. committed snapshot + restore into a new directory with digest/length
//     verification (empty file included),
//  2. small edit reusing existing content-defined chunks,
//  3. refusal to restore over an existing directory,
//  4. file actively written during scan -> re-read then rejected,
//  5. commit interruption losing a blob -> failed snapshot with the exact
//     missing chunk located,
//  6. symlink escaping the root -> restored link is blocked,
//  7. server restart with a pending snapshot -> startup recovery commits it.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

var pass, fail int

func main() {
	keep := flag.Bool("keep", false, "keep the demo workspace afterwards")
	flag.Parse()

	work, err := os.MkdirTemp("", "incbackup-demo-")
	must(err)
	if *keep {
		fmt.Printf("(workspace: %s)\n", work)
	} else {
		defer os.RemoveAll(work)
	}
	repoDir := filepath.Join(work, "repo")
	src := filepath.Join(work, "src")

	section(0, "准备：在一个进程内启动本地 API 服务与数据目录")
	srv, eng := startServer(repoDir)
	fmt.Printf("  API   : %s\n", srv.URL)
	fmt.Printf("  仓库  : %s (manifest.sqlite + chunks/)\n", repoDir)
	fmt.Printf("  数据源: %s\n", src)
	must(os.MkdirAll(filepath.Join(src, "docs"), 0o755))

	// Big-ish log so content-defined chunking produces several chunks.
	log := make([]byte, 0, 160*1024)
	for i := 0; i < 160*1024; i++ {
		log = append(log, byte("abcdefghijklmnopqrstuvwxyz0123456789\n"[i%37]))
	}
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	must(os.WriteFile(filepath.Join(src, "docs", "notes.txt"), []byte("meeting notes\n"), 0o644))
	must(os.WriteFile(filepath.Join(src, "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o750))
	must(os.WriteFile(filepath.Join(src, "EMPTY.dat"), nil, 0o600)) // empty file
	must(os.Symlink("docs/notes.txt", filepath.Join(src, "link_to_notes")))

	// ---- 1. first snapshot + restore --------------------------------------
	section(1, "首次快照：完成前逐块验证，然后恢复到全新目录并核对摘要与长度")
	r := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "baseline"})
	firstID := int64(r["snapshot_id"].(float64))
	fmt.Printf("  快照 %d: status=%s 新块=%v 引用块=%v\n",
		firstID, r["status"], r["chunks_new"], r["chunks_referenced"])
	check("快照状态为 committed", r["status"] == "committed")

	restoreDir := filepath.Join(work, "restore-1")
	code, body := raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #1 failed: HTTP %d %s", code, body["message"]))
	}
	rr := body
	verified, _ := rr["verified"].([]any)
	fmt.Printf("  恢复到 %s\n  文件=%v 目录=%v 符号链接=%v 字节=%v\n",
		restoreDir, rr["files"], rr["directories"], rr["symlinks"], rr["bytes"])
	for _, v := range verified {
		m := v.(map[string]any)
		fmt.Printf("    %-18s 长度=%-6d 块数=%-2d 摘要=%s… 权限=0%o\n",
			m["rel_path"], int64(m["size"].(float64)), int(m["chunk_count"].(float64)),
			m["digest"].(string)[:16], int64(m["mode"].(float64)))
	}
	emptyOK := false
	for _, v := range verified {
		m := v.(map[string]any)
		if m["rel_path"] == "EMPTY.dat" {
			emptyOK = m["size"].(float64) == 0 &&
				m["digest"] == fmt.Sprintf("%x", sha256.New().Sum(nil)) &&
				m["chunk_count"].(float64) == 0
		}
	}
	check("空文件：长度 0、SHA256=e3b0c44…、0 个内容块", emptyOK)

	// Compare tree metadata with source.
	var modeMismatch []string
	for _, rel := range []string{"run.sh", "app.log", "docs"} {
		a, _ := os.Lstat(filepath.Join(src, rel))
		b, err := os.Lstat(filepath.Join(restoreDir, rel))
		if err != nil || a.Mode().Perm() != b.Mode().Perm() {
			modeMismatch = append(modeMismatch, rel)
		}
	}
	check("目录权限与文件权限均保留 (run.sh 0750, docs 0755)", len(modeMismatch) == 0)
	lt, _ := os.Readlink(filepath.Join(restoreDir, "link_to_notes"))
	check("符号链接本身被恢复（链接目标=docs/notes.txt，未跟随）", lt == "docs/notes.txt")
	notes, err := os.ReadFile(filepath.Join(restoreDir, "link_to_notes"))
	check("恢复出的链接仍可解析到文件内容", err == nil && string(notes) == "meeting notes\n")

	// byte-identical content of app.log independently re-hashed
	got, _ := hashFile(filepath.Join(restoreDir, "app.log"))
	want, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复内容逐字节一致（独立重算 SHA256）", got == want)

	// ---- 2. small edit reuses chunks --------------------------------------
	section(2, "小改动的增量：在 app.log 中部改一行，只新增 1 个块，其余块全部复用")
	off := 80 * 1024
	copy(log[off:off+8], []byte("PATCHED!"))
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	r = post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "one-line patch"})
	secondID := int64(r["snapshot_id"].(float64))
	newChunks := int64(r["chunks_new"].(float64))
	refChunks := int64(r["chunks_referenced"].(float64))
	fmt.Printf("  快照 %d: 引用块=%d，其中新写入=%d，复用=%d\n",
		secondID, refChunks, newChunks, refChunks-newChunks)
	check("仅有改动附近的 1 个块是新块（内容定义分块边界由内容决定）", newChunks == 1)
	check("其余块全部复用快照 1 中的旧块", refChunks-newChunks == refChunks-1)

	restore2 := filepath.Join(work, "restore-2")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", secondID), map[string]any{"target": restore2})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #2 failed: HTTP %d %s", code, body["message"]))
	}
	g2, _ := hashFile(filepath.Join(restore2, "app.log"))
	w2, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复快照 2 后 app.log 与当前源文件一致", g2 == w2)

	// ---- 3. never overwrite destination -----------------------------------
	section(3, "恢复位置已有任何东西 → 拒绝，不覆盖、不合并")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	fmt.Printf("  POST restore 到已存在目录 -> HTTP %d: %s\n", code, body["error"])
	check("已有目录时返回 409 target_exists", code == http.StatusConflict && body["error"] == "target_exists")

	// ---- 4. file being written during scan --------------------------------
	section(4, "扫描中仍在写入的文件：先短暂写入触发重读，再持续写入触发拒绝")
	growing := filepath.Join(src, "growing.log")
	must(os.WriteFile(growing, []byte("line0\n"), 0o644))

	// 4a. writer finishes within the retry window: scanner re-reads and commits
	done := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		for i := 1; i <= 5; i++ {
			fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("y", 200))
			time.Sleep(20 * time.Millisecond)
		}
		f.Close()
		close(done)
	}()
	snap4a := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "writer settles during retry"})
	<-done
	check("写入在重读窗口内结束：扫描器重读文件，快照仍正常 committed", snap4a["status"] == "committed")

	// 4b. writer keeps going: every pass sees a changed size/mtime -> rejected
	stop := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		defer f.Close()
		i := 1
		for {
			select {
			case <-stop:
				return
			default:
				fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("x", 200))
				i++
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	time.Sleep(30 * time.Millisecond)
	code, body = raw("POST", srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "racing writer"})
	close(stop)
	reasons, _ := body["reasons"].([]any)
	fmt.Printf("  持续写入时 HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	for _, x := range reasons {
		fmt.Printf("    拒绝原因: %s\n", x)
	}
	sawUnstable := false
	for _, x := range reasons {
		if strings.Contains(x.(string), "growing.log") &&
			strings.Contains(x.(string), "still being written") {
			sawUnstable = true
		}
	}
	check("3 次重读后仍在变化的 growing.log 被明确点名（而不是备份静默成功）",
		code == http.StatusConflict && sawUnstable)
	badID := int64(body["snapshot_id"].(float64))
	errs, _ := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/errors", badID))["errors"].([]any)
	check("失败快照保留在清单中，stage=scan 可追溯", len(errs) > 0 &&
		errs[0].(map[string]any)["stage"] == "scan")

	// ---- 5. commit interruption: lost blob, locate exact chunk ------------
	section(5, "模拟提交中断：删掉最后一个内容块 → 完成前验证拦截并定位具体缺块")
	must(os.WriteFile(growing, []byte("stable now\n"), 0o644))
	code, body = raw("POST", srv.URL+"/v1/snapshots",
		map[string]any{"root": src, "message": "interrupted commit", "lose_chunks": 1})
	fmt.Printf("  HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	interruptedID := int64(body["snapshot_id"].(float64))
	check("缺块快照不能 committed，返回 409", code == http.StatusConflict)

	missing := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/missing", interruptedID))["missing"].([]any)
	fmt.Printf("  维护查询 GET .../missing 找到 %d 个缺块：\n", len(missing))
	for _, x := range missing {
		m := x.(map[string]any)
		fmt.Printf("    文件   : %s\n", m["rel_path"])
		fmt.Printf("    块摘要 : %s\n", m["chunk_digest"])
		fmt.Printf("    应在   : %s\n", m["expected_blob_path"])
		fmt.Printf("    原因   : %s\n", m["reason"])
		_, statErr := os.Stat(m["expected_blob_path"].(string))
		check("报告的块路径在磁盘上确实不存在", os.IsNotExist(statErr))
	}
	check("缺块清单精确到 文件+摘要+期望磁盘路径（不是“上传队列为空”）", len(missing) == 1)
	si := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", interruptedID))
	check("失败快照状态可查 = failed", si["status"] == "failed")

	// restore of a failed snapshot must be refused
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", interruptedID),
		map[string]any{"target": filepath.Join(work, "never")})
	fmt.Printf("  尝试恢复 failed 快照 -> HTTP %d %s\n", code, body["error"])
	check("failed 快照拒绝恢复", code >= 400)

	// ---- 6. symlink escape containment ------------------------------------
	section(6, "符号链接越界：备份只存链接本身，恢复时指向根目录外的链接被拒绝")
	secret := filepath.Join(work, "secret.txt")
	must(os.WriteFile(secret, []byte("TOP SECRET"), 0o600))
	evil := filepath.Join(src, "evil_link")
	_ = os.Remove(evil)
	rel, _ := filepath.Rel(filepath.Join(src), secret)
	must(os.Symlink(rel, evil)) // src/evil_link -> ../secret.txt
	snapEvil := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "with evil link"})
	evilID := int64(snapEvil["snapshot_id"].(float64))
	evilTarget := filepath.Join(work, "restore-evil")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", evilID),
		map[string]any{"target": evilTarget})
	fmt.Printf("  含越界链接的恢复 -> HTTP %d: %s\n", code, body["message"])
	check("越界符号链接恢复被阻止 (422)", code == http.StatusUnprocessableEntity)
	_, statErr := os.Lstat(evilTarget)
	check("失败后不留半成品目录（回滚清理）", os.IsNotExist(statErr))
	_, err = os.ReadFile(filepath.Join(evilTarget, "evil_link"))
	check("秘密文件没有被触及/写出", err != nil)

	// ---- 7. restart recovery of a pending snapshot -------------------------
	section(7, "提交前进程退出：快照留在 pending，服务重启时自动验证并给结论")
	pend := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "crash before commit", "finish": false})
	pendID := int64(pend["snapshot_id"].(float64))
	fmt.Printf("  故障时刻: 快照 %d status=%s，块已落盘、清单未提交\n", pendID, pend["status"])
	check("finish=false 留下 pending 快照", pend["status"] == "pending")
	srv.Close()

	srv, eng = startServer(repoDir) // same repo, new process equivalent
	time.Sleep(100 * time.Millisecond)
	si = get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", pendID))
	fmt.Printf("  重启后: 快照 %d status=%s\n", pendID, si["status"])
	check("重启恢复把 pending 快照验证后提交为 committed", si["status"] == "committed")

	// ---- 8. selective restore jobs ------------------------------------------
	section(8, "选择性恢复：冻结路径集合、暂存区校验后原子发布、幂等、失败定位与断点恢复")

	// 8.1 one deep file -> only it and its parent directories are restored
	partial1 := filepath.Join(work, "partial-1")
	code, body = raw("POST", srv.URL+"/v1/restore-jobs", map[string]any{
		"snapshot_id": firstID, "target": partial1,
		"paths": []string{"docs/notes.txt"}, "idempotency_key": "demo-p1",
	})
	check("创建选择性恢复作业 -> 201", code == http.StatusCreated)
	job := body["job"].(map[string]any)
	check("作业状态 done", job["status"] == "done")
	jfiles, _ := body["files"].([]any)
	fmt.Printf("  作业 %v: 计划 %d 行（选中文件 + 自动补足的父目录）\n", job["id"], len(jfiles))
	for _, x := range jfiles {
		m := x.(map[string]any)
		fmt.Printf("    %-18s kind=%-8s implicit=%-5v status=%s\n", m["rel_path"], m["kind"], m["implicit"], m["status"])
	}
	check("计划只含 docs/（隐式父目录）与 docs/notes.txt", len(jfiles) == 2)
	g1, _ := hashFile(filepath.Join(partial1, "docs", "notes.txt"))
	w1, _ := hashFile(filepath.Join(restoreDir, "docs", "notes.txt"))
	check("摘要与整树恢复结果一致", g1 == w1)
	fi1, _ := os.Lstat(filepath.Join(partial1, "docs", "notes.txt"))
	srcNotes, _ := os.Lstat(filepath.Join(src, "docs", "notes.txt"))
	check("权限与 mtime 保留", fi1.Mode().Perm() == srcNotes.Mode().Perm() &&
		fi1.ModTime().Equal(srcNotes.ModTime()))
	_, err = os.Lstat(filepath.Join(partial1, "app.log"))
	check("未选中的 app.log 不存在", os.IsNotExist(err))
	_, err = os.Lstat(filepath.Join(partial1, "run.sh"))
	check("未选中的 run.sh 不存在", os.IsNotExist(err))

	// 8.2 missing link target -> precheck failure, nothing created
	partial2 := filepath.Join(work, "partial-2")
	code, body = raw("POST", srv.URL+"/v1/restore-jobs", map[string]any{
		"snapshot_id": firstID, "target": partial2,
		"paths": []string{"link_to_notes"}, "idempotency_key": "demo-p2",
	})
	fmt.Printf("  只选链接不选目标 -> HTTP %d: %s\n", code, body["message"])
	check("预检拒绝并返回 422 missing_link_deps", code == http.StatusUnprocessableEntity &&
		body["error"] == "missing_link_deps")
	missDeps, _ := body["details"].(map[string]any)["missing"].([]any)
	check("错误点名应一并纳入的依赖 docs/notes.txt", len(missDeps) == 1 && missDeps[0] == "docs/notes.txt")
	_, err = os.Lstat(partial2)
	check("预检失败：目标目录不存在", os.IsNotExist(err))
	code, body = raw("POST", srv.URL+"/v1/restore-jobs", map[string]any{
		"snapshot_id": firstID, "target": partial2,
		"paths": []string{"link_to_notes", "docs/notes.txt"}, "idempotency_key": "demo-p2",
	})
	check("把依赖纳入选择后 -> 201 done", code == http.StatusCreated &&
		body["job"].(map[string]any)["status"] == "done")
	lt, _ = os.Readlink(filepath.Join(partial2, "link_to_notes"))
	check("链接目标逐字保留 docs/notes.txt（绝不重写）", lt == "docs/notes.txt")

	// 8.3 idempotency: same key -> same job; existing target -> 409
	code, body = raw("POST", srv.URL+"/v1/restore-jobs", map[string]any{
		"snapshot_id": firstID, "target": partial1,
		"paths": []string{"docs/notes.txt"}, "idempotency_key": "demo-p1",
	})
	check("同一幂等键重放 -> 200 且返回同一作业", code == http.StatusOK &&
		body["idempotent_replay"] == true && body["job"].(map[string]any)["id"] == job["id"])
	code, body = raw("POST", srv.URL+"/v1/restore-jobs", map[string]any{
		"snapshot_id": firstID, "target": partial1,
		"paths": []string{"docs/notes.txt"}, "idempotency_key": "demo-other-key",
	})
	check("已有目标仍返回 409 target_exists", code == http.StatusConflict && body["error"] == "target_exists")
	stagingLeft, _ := filepath.Glob(filepath.Join(work, ".incbackup-restore-staging-*"))
	check("不产生第二棵树、不留暂存半成品", len(stagingLeft) == 0)

	// 8.4 lost chunk -> job locates the failure; repair -> retry -> done
	must(os.WriteFile(filepath.Join(src, "unique.bin"), []byte(strings.Repeat("UNIQUE-", 20000)), 0o644))
	r = post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "for selective failure drill"})
	selID := int64(r["snapshot_id"].(float64))
	var victimDigest []byte
	entries, err := eng.Manifest.EntriesOf(selID)
	must(err)
	for _, se := range entries {
		if se.RelPath == "unique.bin" {
			victimDigest = se.ChunkDigests[0]
		}
	}
	blobPath, err := eng.Store.Path(victimDigest)
	must(err)
	savedBlob, err := os.ReadFile(blobPath)
	must(err)
	must(os.Remove(blobPath)) // simulate storage loss of one chunk
	fmt.Printf("  故障注入: 删除 unique.bin 的块 %x…\n", victimDigest[:8])

	partial3 := filepath.Join(work, "partial-3")
	code, body = raw("POST", srv.URL+"/v1/restore-jobs", map[string]any{
		"snapshot_id": selID, "target": partial3,
		"paths": []string{"unique.bin"}, "idempotency_key": "demo-p3",
	})
	check("缺块时 -> 409 restore_job_failed", code == http.StatusConflict && body["error"] == "restore_job_failed")
	failedJob := body["job"].(map[string]any)
	check("作业持久化为 failed 且错误点名 unique.bin", failedJob["status"] == "failed" &&
		strings.Contains(failedJob["error"].(string), "unique.bin"))
	jfiles, _ = body["files"].([]any)
	located := false
	for _, x := range jfiles {
		m := x.(map[string]any)
		if m["rel_path"] == "unique.bin" && m["status"] == "failed" &&
			strings.Contains(m["error"].(string), fmt.Sprintf("%x", victimDigest)[:16]) {
			located = true
			fmt.Printf("  逐文件报告定位: %s -> %s\n", m["rel_path"], m["error"])
		}
	}
	check("逐文件报告定位到具体文件与块摘要", located)
	_, err = os.Lstat(partial3)
	check("失败不留目标目录", os.IsNotExist(err))
	stagingLeft, _ = filepath.Glob(filepath.Join(work, ".incbackup-restore-staging-*"))
	check("失败不留暂存区", len(stagingLeft) == 0)

	must(os.WriteFile(blobPath, savedBlob, 0o444)) // maintenance repairs the blob
	failedJobID := int64(failedJob["id"].(float64))
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/restore-jobs/%d/retry", failedJobID), nil)
	check("修复缺块后重试同一作业 -> 200 done", code == http.StatusOK &&
		body["job"].(map[string]any)["status"] == "done")
	gb, _ := hashFile(filepath.Join(partial3, "unique.bin"))
	wb, _ := hashFile(filepath.Join(src, "unique.bin"))
	check("重试后内容逐字节一致", gb == wb)
	code, _ = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", secondID),
		map[string]any{"target": filepath.Join(work, "restore-after-drill")})
	check("其他已提交快照仍可正常整树恢复", code == http.StatusCreated)

	// 8.5 crash mid-job -> restart cleans its own staging, job retryable
	partial4 := filepath.Join(work, "partial-4")
	crashStaging := filepath.Join(work, ".incbackup-restore-staging-crash01")
	crashJobID, err := eng.Manifest.InsertRestoreJob(&repo.RestoreJob{
		IdempotencyKey: "demo-crash", SnapshotID: firstID,
		Target: partial4, Staging: crashStaging, RequestedPaths: []string{"run.sh"},
	})
	must(err)
	must(eng.Manifest.UpdateRestoreJobStatus(crashJobID, repo.JobRunning, ""))
	must(os.MkdirAll(filepath.Join(crashStaging, "half-built"), 0o700))
	fmt.Printf("  故障注入: 作业 %d 崩溃在 running，留下暂存区 %s\n", crashJobID, filepath.Base(crashStaging))
	srv.Close()

	srv, eng = startServer(repoDir) // startup recovery runs
	cj := get(srv.URL + fmt.Sprintf("/v1/restore-jobs/%d", crashJobID))["job"].(map[string]any)
	check("崩溃的 running 作业被标记 failed（可重试）", cj["status"] == "failed")
	_, err = os.Lstat(crashStaging)
	check("服务只清理自己创建的暂存区", os.IsNotExist(err))
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/restore-jobs/%d/retry", crashJobID), nil)
	check("重试后作业完成", code == http.StatusOK && body["job"].(map[string]any)["status"] == "done")
	g3, _ := hashFile(filepath.Join(partial4, "run.sh"))
	w3, _ := hashFile(filepath.Join(src, "run.sh"))
	check("恢复内容正确", g3 == w3)

	// final listing
	section(0, "快照总览")
	list := get(srv.URL + "/v1/snapshots")["snapshots"].([]any)
	for _, x := range list {
		m := x.(map[string]any)
		fmt.Printf("  #%-3v %-10s files=%-3v bytes=%-7v %s\n",
			m["id"], m["status"], m["file_count"], m["bytes_total"], m["message"])
	}
	srv.Close()

	fmt.Println()
	if fail == 0 {
		fmt.Printf("✅ 全部 %d 项检查通过\n", pass)
		return
	}
	fmt.Printf("❌ %d 项失败，%d 项通过\n", fail, pass)
	os.Exit(1)
}

// ---------- helpers ----------

func startServer(repoDir string) (*httptest.Server, *backup.Engine) {
	must(os.MkdirAll(repoDir, 0o755))
	manifest, err := repo.OpenManifest(filepath.Join(repoDir, "manifest.sqlite"))
	must(err)
	store, err := repo.NewContentStore(filepath.Join(repoDir, "chunks"))
	must(err)
	engine, err := backup.NewEngine(manifest, store)
	must(err)
	if recovered, err := engine.RecoverPending(); err == nil {
		for _, r := range recovered {
			fmt.Printf("  [启动恢复] 快照 %d -> %s\n", r.SnapshotID, r.Status)
		}
	}
	if jobs, err := engine.RecoverRestoreJobs(); err == nil {
		for _, j := range jobs {
			fmt.Printf("  [启动恢复] 恢复作业 %d -> %s\n", j.ID, j.Status)
		}
	}
	return httptest.NewServer((&api.Server{Engine: engine}).NewRouter()), engine
}

func post(url string, body any) map[string]any {
	code, b := raw("POST", url, body)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func get(url string) map[string]any {
	code, b := raw("GET", url, nil)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func raw(method, url string, body any) (int, map[string]any) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		must(err)
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, rdr)
	must(err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	must(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	must(err)
	var out map[string]any
	if len(data) > 0 {
		must(json.Unmarshal(data, &out))
		if out == nil {
			out = map[string]any{}
		}
	} else {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func hashFile(p string) (string, int64) {
	f, err := os.Open(p)
	must(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	must(err)
	return hex.EncodeToString(h.Sum(nil)), n
}

func section(n int, title string) {
	if n == 0 {
		fmt.Printf("\n── %s ──────────────────────────────\n", title)
		return
	}
	fmt.Printf("\n── %d. %s ──────────────────────────────\n", n, title)
}

func check(name string, ok bool) {
	if ok {
		pass++
		fmt.Printf("  ✓ %s\n", name)
		return
	}
	fail++
	fmt.Printf("  ✗ %s\n", name)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		var ee *exec.ExitError
		_ = ee
		os.Exit(2)
	}
}
