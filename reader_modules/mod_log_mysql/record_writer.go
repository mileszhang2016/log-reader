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
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
)

// retryBackoffMs 写失败重试的指数退避间隔（毫秒）：200ms/400ms/800ms
var retryBackoffMs = []int64{200, 400, 800}

// connector 建立并验证一个 MySQL 连接（open + 超时 ping + 连接池参数），
// 作为 RecordWriter 的注入缝：单测替换为"前 N 次失败、第 N+1 次成功"的桩
type connector func(conf *ConfModLogMysql) (*sql.DB, error)

// defaultConnector 用真实 MySQL 驱动建连；失败时不残留句柄，可反复调用
func defaultConnector(conf *ConfModLogMysql) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4",
		conf.Mysql.User, conf.Mysql.Password, conf.Mysql.Addr, conf.Mysql.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// PingContext 超时控制：目标 IP 被黑洞时 db.Ping() 可能卡到 OS 级 TCP
	// 超时（分钟级），用 ConnectTimeoutMs 约束单次尝试上限，重试节奏才可控
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(conf.Mysql.ConnectTimeoutMs)*time.Millisecond)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	db.SetMaxOpenConns(conf.Writer.MaxOpenConns)
	db.SetMaxIdleConns(conf.Writer.MaxIdleConns)
	return db, nil
}

// RecordWriter 批量写入器：缓冲队列 + 单写协程攒批 + 事务批插 + 退避重试。
// 建连由 Connect/connectLoop 负责：启动时 MySQL 不可达不致命，后台持续重试，
// 建连成功前 writeLoop 挂起不消费，行数据在缓冲队列中堆积（背压见 Enqueue）。
type RecordWriter struct {
	db       atomic.Pointer[sql.DB] // nil = 未连通；connectLoop 写、writeLoop/Close 读
	connect  connector
	conf     *ConfModLogMysql
	ch       chan []interface{} // 容量 QueueSize
	state    *module_state2.State
	stopCh   chan struct{}
	connCh   chan struct{} // 建连成功信号，close 一次（connOnce 保护）
	connOnce sync.Once
	wg       sync.WaitGroup

	stmtHead string // INSERT INTO <table> (cols...) VALUES
	stmtRow  string // (?,...,?)，按列数预生成
	stmtTail string // ON DUPLICATE KEY UPDATE col=VALUES(col),...（全列覆盖）
}

// NewRecordWriter 创建 RecordWriter（不启动协程，需调用 Start；
// 连接由 Connect 建立，可在 Start 前或 connectLoop 中反复调用）
func NewRecordWriter(conf *ConfModLogMysql, state *module_state2.State) *RecordWriter {
	w := &RecordWriter{
		connect: defaultConnector,
		conf:    conf,
		ch:      make(chan []interface{}, conf.Writer.QueueSize),
		state:   state,
		stopCh:  make(chan struct{}),
		connCh:  make(chan struct{}),
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

// Connect 建立连接并验证连通性；失败不残留句柄，可反复调用。
// 成功时唤醒挂起的 writeLoop
func (w *RecordWriter) Connect() error {
	if w.Connected() {
		return nil
	}

	db, err := w.connect(w.conf)
	if err != nil {
		return err
	}

	w.db.Store(db)
	w.connOnce.Do(func() { close(w.connCh) })
	return nil
}

// Connected 报告当前是否已建连
func (w *RecordWriter) Connected() bool {
	return w.db.Load() != nil
}

// Start 启动写协程；未连通时同时启动建连重试协程
func (w *RecordWriter) Start() {
	w.wg.Add(1)
	go w.writeLoop()
	if !w.Connected() {
		w.wg.Add(1)
		go w.connectLoop()
	}
}

// connectLoop 建连重试协程：每 ConnectRetryIntervalMs 重试一次 Connect，
// 成功则计数、置状态并退出（唤醒 writeLoop 后排空积压），失败按节流规则记日志
func (w *RecordWriter) connectLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(time.Duration(w.conf.Mysql.ConnectRetryIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	var attempts int
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
		}

		attempts++
		if err := w.Connect(); err != nil {
			w.state.Inc("MYSQL_CONN_RETRY", 1)
			// 日志节流：第 1-3 次 Error，第 4 次起每 10 次 Warn 一条
			switch {
			case attempts <= 3:
				log.Logger.Error("mod_log_mysql: connect retry %d failed: %v", attempts, err)
			case attempts%10 == 0:
				log.Logger.Warn("mod_log_mysql: connect retry %d failed: %v", attempts, err)
			}
			continue
		}

		w.state.Inc("MYSQL_CONN_OK", 1)
		w.state.Set("MYSQL_CONN_STATE", "UP")
		log.Logger.Info("mod_log_mysql: connected to mysql (after %d retries), draining buffered records", attempts)
		return
	}
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

	// 闸：未连通前挂起、不消费 ch，行数据在缓冲队列中堆积（背压见 Enqueue）。
	// Close 可在建连前到达；若建连与关闭几乎同时，优先按已连通处理——
	// 主循环会立即命中 stopCh 并走 drain 兜底，避免残留行丢失
	select {
	case <-w.connCh:
	case <-w.stopCh:
		select {
		case <-w.connCh:
		default:
			return
		}
	}

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
	db := w.db.Load()
	if db == nil {
		return fmt.Errorf("mod_log_mysql: not connected")
	}

	args := make([]interface{}, 0, len(batch)*len(Columns()))
	for _, row := range batch {
		args = append(args, row...)
	}

	txn, err := db.Begin()
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

// Close 停协程（connectLoop 直接退出，writeLoop drain 残留批后最终 flush）：
// 已连通关闭连接池；未连通丢弃缓冲行并记 Info 日志
func (w *RecordWriter) Close() {
	close(w.stopCh)
	w.wg.Wait()

	if db := w.db.Load(); db != nil {
		if err := db.Close(); err != nil {
			log.Logger.Error("mod_log_mysql: close db failed: %v", err)
		}
	} else if n := len(w.ch); n > 0 {
		log.Logger.Info("mod_log_mysql: closed with %d buffered records, dropped (never connected)", n)
	}
}
