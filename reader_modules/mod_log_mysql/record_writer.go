// Copyright(c) 2026 The Rainway AI Gateway (壬远AI网关) Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package mod_log_mysql

import (
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
)

// retryBackoffMs 写失败重试的指数退避间隔（毫秒）：200ms/400ms/800ms
var retryBackoffMs = []int64{200, 400, 800}

// RecordWriter 批量写入器：缓冲队列 + 单写协程攒批 + 事务批插 + 退避重试
type RecordWriter struct {
	db     *sql.DB
	conf   *ConfModLogMysql
	ch     chan []interface{} // 容量 QueueSize
	state  *module_state2.State
	stopCh chan struct{}
	wg     sync.WaitGroup

	stmtHead string // INSERT INTO <table> (cols...) VALUES
	stmtRow  string // (?,...,?)，按列数预生成
	stmtTail string // ON DUPLICATE KEY UPDATE col=VALUES(col),...（全列覆盖）
}

// NewRecordWriter 创建 RecordWriter（不启动写协程，需调用 Start）
func NewRecordWriter(db *sql.DB, conf *ConfModLogMysql, state *module_state2.State) *RecordWriter {
	w := &RecordWriter{
		db:     db,
		conf:   conf,
		ch:     make(chan []interface{}, conf.Writer.QueueSize),
		state:  state,
		stopCh: make(chan struct{}),
	}

	cols := Columns()
	w.stmtHead = "INSERT INTO " + conf.Mysql.Table + " (" + strings.Join(cols, ",") + ") VALUES "
	w.stmtRow = "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"

	var sb strings.Builder
	sb.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, col := range cols {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(col + "=VALUES(" + col + ")")
	}
	w.stmtTail = sb.String()

	return w
}

// buildStmt 按批条数组装 INSERT 语句
func (w *RecordWriter) buildStmt(n int) string {
	var sb strings.Builder
	sb.WriteString(w.stmtHead)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(w.stmtRow)
	}
	sb.WriteString(w.stmtTail)
	return sb.String()
}

// Enqueue 非阻塞入队一行，队列满返回 false
func (w *RecordWriter) Enqueue(row []interface{}) bool {
	select {
	case w.ch <- row:
		return true
	default:
		return false
	}
}

// Start 启动单写协程
func (w *RecordWriter) Start() {
	w.wg.Add(1)
	go w.writeLoop()
}

// writeLoop 写协程：攒批（BatchSize 条或 FlushIntervalMs 先到先发）
func (w *RecordWriter) writeLoop() {
	defer func() {
		if r := recover(); r != nil {
			w.state.Inc("SEND_MYSQL_FAILED", 1)
			log.Logger.Error("mod_log_mysql: writeLoop panic: %v", r)
		}
		w.wg.Done()
	}()

	ticker := time.NewTicker(time.Duration(w.conf.Writer.FlushIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	batch := make([][]interface{}, 0, w.conf.Writer.BatchSize)
	flush := func(prompt string) {
		if len(batch) == 0 {
			return
		}
		if openDebug {
			log.Logger.Debug("mod_log_mysql: flush to mysql. prompt:%s, batch size:%d", prompt, len(batch))
		}
		w.writeBatchWithRetry(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-w.stopCh:
			// drain 残留数据后最终 flush
			for {
				select {
				case row := <-w.ch:
					batch = append(batch, row)
					if len(batch) >= w.conf.Writer.BatchSize {
						flush("drain")
					}
				default:
					flush("close")
					return
				}
			}

		case row := <-w.ch:
			batch = append(batch, row)
			if len(batch) >= w.conf.Writer.BatchSize {
				flush("batch")
			}

		case <-ticker.C:
			flush("timeout")
		}
	}
}

// writeBatchWithRetry 单事务批插，失败按 200ms/400ms/800ms 退避重试至 MaxRetries，
// 耗尽则计数 SEND_MYSQL_FAILED 丢弃该批
func (w *RecordWriter) writeBatchWithRetry(batch [][]interface{}) {
	var err error
	for attempt := 0; attempt <= w.conf.Writer.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := retryBackoffMs[attempt-1]
			if attempt-1 >= len(retryBackoffMs) {
				backoff = retryBackoffMs[len(retryBackoffMs)-1]
			}
			time.Sleep(time.Duration(backoff) * time.Millisecond)
		}

		err = w.writeBatch(batch)
		if err == nil {
			w.state.Inc("WRITE_BATCH_SIZE", len(batch))
			return
		}
		log.Logger.Error("mod_log_mysql: write batch failed (attempt %d/%d): %v",
			attempt+1, w.conf.Writer.MaxRetries+1, err)
	}

	w.state.Inc("SEND_MYSQL_FAILED", 1)
	log.Logger.Error("mod_log_mysql: write batch failed after %d retries, dropped %d rows",
		w.conf.Writer.MaxRetries, len(batch))
}

// writeBatch 单事务执行多值 INSERT ... ON DUPLICATE KEY UPDATE
func (w *RecordWriter) writeBatch(batch [][]interface{}) error {
	args := make([]interface{}, 0, len(batch)*len(Columns()))
	for _, row := range batch {
		args = append(args, row...)
	}

	txn, err := w.db.Begin()
	if err != nil {
		return err
	}

	_, err = txn.Exec(w.buildStmt(len(batch)), args...)
	if err != nil {
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

// Close 停 ticker -> drain 残留批 -> 最终 flush -> 关闭连接池
func (w *RecordWriter) Close() {
	close(w.stopCh)
	w.wg.Wait()

	if w.db != nil {
		if err := w.db.Close(); err != nil {
			log.Logger.Error("mod_log_mysql: close db failed: %v", err)
		}
	}
}
