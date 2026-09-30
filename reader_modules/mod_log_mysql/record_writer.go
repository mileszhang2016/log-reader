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

// defaultConnector 用真实 MySQL 驱动建连；失败时不残留句柄，可反复调用。
// DSN 中 timeout（拨号）与 writeTimeout（写操作，含 Commit/Rollback）防止
// 网络分区时写路径挂到 OS 级 TCP 超时
func defaultConnector(conf *ConfModLogMysql) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&timeout=%dms&writeTimeout=%dms",
		conf.Mysql.User, conf.Mysql.Password, conf.Mysql.Addr, conf.Mysql.DBName,
		conf.Mysql.ConnectTimeoutMs, conf.Writer.BatchTimeoutMs)

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
// 建连由 connSupervisor 负责（启动未连通、运行期断连均后台重试，Connect 幂等
// 可反复调用）；writeLoop 在建连信号到达前挂起不消费，行数据在缓冲队列中
// 堆积（背压见 Enqueue）。连接状态机：UP --(写重试耗尽/ping 失败)--> DOWN
// --(supervisor 重连成功)--> UP
type RecordWriter struct {
	db      atomic.Pointer[sql.DB] // nil = DOWN；markDown 写、Connect/supervisor 写、writeLoop/Close 读
	connect connector
	conf    *ConfModLogMysql
	ch      chan []interface{} // 容量 QueueSize
	state   *module_state2.State
	stopCh  chan struct{}
	discCh  chan struct{} // markDown 唤醒 supervisor 的信号（容量 1，非阻塞发送）
	connMu  sync.RWMutex  // 保护 connCh 换代
	connCh  chan struct{} // 当前代建连信号：Connect 成功时 close 旧 channel 并换新，等待者按代唤醒
	wg      sync.WaitGroup

	stmtHead string // INSERT INTO <table> (cols...) VALUES
	stmtRow  string // (?,...,?)，按列数预生成
	stmtTail string // ON DUPLICATE KEY UPDATE col=VALUES(col),...（全列覆盖）
}

// NewRecordWriter 创建 RecordWriter（不启动协程，需调用 Start；
// 连接由 Connect 建立，可在 Init、supervisor 重试中反复调用）
func NewRecordWriter(conf *ConfModLogMysql, state *module_state2.State) *RecordWriter {
	w := &RecordWriter{
		connect: defaultConnector,
		conf:    conf,
		ch:      make(chan []interface{}, conf.Writer.QueueSize),
		state:   state,
		stopCh:  make(chan struct{}),
		discCh:  make(chan struct{}, 1),
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

// Connect 建立连接并验证连通性；失败不残留句柄，可反复调用（supervisor 专用入口，
// 运行期单调用者）。成功时换代 connCh：close 当前代唤醒全部等待者，再开新代
func (w *RecordWriter) Connect() error {
	if w.Connected() {
		return nil
	}

	db, err := w.connect(w.conf)
	if err != nil {
		return err
	}

	old := w.db.Swap(db)
	if old != nil {
		// 与 markDown 并发的补偿路径：换代前清掉旧池，保证至多一个有效池
		old.Close()
	}

	w.connMu.Lock()
	close(w.connCh)
	w.connCh = make(chan struct{})
	w.connMu.Unlock()
	return nil
}

// Connected 报告当前是否已建连（UP）
func (w *RecordWriter) Connected() bool {
	return w.db.Load() != nil
}

// currentConnCh 返回当前代的建连信号 channel
func (w *RecordWriter) currentConnCh() chan struct{} {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	return w.connCh
}

// waitConnected 挂起至建连成功（返回 true）或 Close（返回 false）。
// 双重检查消除"读取 channel 与 Connect 换代"之间的竞态
func (w *RecordWriter) waitConnected() bool {
	if w.Connected() {
		return true
	}
	ch := w.currentConnCh()
	if w.Connected() {
		return true
	}
	select {
	case <-ch:
		return true
	case <-w.stopCh:
		return false
	}
}

// markDown 置 DOWN 的唯一入口：摘除并关闭连接池、计数、唤醒 supervisor 重连。
// 与 Connect 的 db.Swap 互斥——任何时刻至多一个有效池，先到者生效，
// 后到者拿到 nil 直接返回（幂等，不重复计数）
func (w *RecordWriter) markDown(reason string) {
	db := w.db.Swap(nil)
	if db == nil {
		return
	}
	db.Close()

	w.state.Set("MYSQL_CONN_STATE", "DOWN")
	w.state.Inc("MYSQL_CONN_LOST", 1)
	log.Logger.Error("mod_log_mysql: connection lost (%s), reconnecting in background", reason)

	select {
	case w.discCh <- struct{}{}:
	default:
	}
}

// Start 启动写协程与连接监管协程（均常驻；建连失败/运行期断连由 supervisor 重试）
func (w *RecordWriter) Start() {
	w.wg.Add(2)
	go w.writeLoop()
	go w.connSupervisor()
}

// connSupervisor 连接监管协程（常驻）：UP 期等待 markDown 触发（可选周期探活），
// DOWN 期按 ConnectRetryIntervalMs 重试 Connect 直至成功；退出受 stopCh 控制
func (w *RecordWriter) connSupervisor() {
	defer w.wg.Done()

	ticker := time.NewTicker(time.Duration(w.conf.Mysql.ConnectRetryIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	// 可选健康探活：HealthPingIntervalMs > 0 时启用，半开连接场景更早发现断连
	var pingC <-chan time.Time
	var pingTicker *time.Ticker
	if w.conf.Mysql.HealthPingIntervalMs > 0 {
		pingTicker = time.NewTicker(time.Duration(w.conf.Mysql.HealthPingIntervalMs) * time.Millisecond)
		pingC = pingTicker.C
	}
	if pingTicker != nil {
		defer pingTicker.Stop()
	}

	attempts := 0 // 当前 DOWN 周期内的重试次数（每轮 UP 清零），用于日志节流分母
	for {
		if w.Connected() {
			attempts = 0
			select {
			case <-w.stopCh:
				return
			case <-w.discCh:
				// markDown 已触发，进入下方 DOWN 重试
			case <-pingC:
				if db := w.db.Load(); db != nil {
					ctx, cancel := context.WithTimeout(context.Background(),
						time.Duration(w.conf.Mysql.ConnectTimeoutMs)*time.Millisecond)
					err := db.PingContext(ctx)
					cancel()
					if err != nil {
						w.markDown("health ping failed")
					}
				}
			}
			continue
		}

		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
		}

		if err := w.Connect(); err != nil {
			w.state.Inc("MYSQL_CONN_RETRY", 1)
			attempts++
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
	}
}

// writeLoop 写协程：攒批（BatchSize 条或 FlushIntervalMs 先到先发）。
// DOWN 期挂起不消费（行数据在缓冲队列堆积，背压见 Enqueue），UP 后恢复
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
		// 每个写周期前确保连通；DOWN 时挂起（可反复进入），Close 时退出
		if !w.waitConnected() {
			return
		}

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

// writeBatchWithRetry 单事务批插，失败按 200ms/400ms/800ms 退避重试至 MaxRetries。
// 耗尽即判定连接失效：markDown 置 DOWN 触发 supervisor 重连，并持有该批
// 待重连成功后优先补写（不占 QueueSize、保序优于 re-enqueue）；补写仍失败
// 才计数 SEND_MYSQL_FAILED 丢弃。运行期断连因此不再线性丢批
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

		if !w.Connected() {
			err = fmt.Errorf("mod_log_mysql: not connected")
			break
		}

		err = w.writeBatch(batch)
		if err == nil {
			w.state.Inc("WRITE_BATCH_SIZE", len(batch))
			return
		}
		log.Logger.Error("mod_log_mysql: write batch failed (attempt %d/%d): %v",
			attempt+1, w.conf.Writer.MaxRetries+1, err)
	}

	w.markDown("write retry exhausted")

	// 持有该批，等待重连成功后优先补写；Close 到达则批随进程消亡（既有语义）
	if !w.waitConnected() {
		return
	}
	if err := w.writeBatch(batch); err == nil {
		w.state.Inc("WRITE_BATCH_SIZE", len(batch))
		return
	}
	w.state.Inc("SEND_MYSQL_FAILED", 1)
	log.Logger.Error("mod_log_mysql: rewrite after reconnect failed, dropped %d rows", len(batch))
}

// writeBatch 单事务执行多值 INSERT ... ON DUPLICATE KEY UPDATE。
// 事务带 BatchTimeoutMs 超时：BeginTx 约束拨号、ExecContext 约束执行；
// Commit/Rollback 由 DSN writeTimeout 同值约束（stdlib 的 sql.Tx 无 CommitContext）
func (w *RecordWriter) writeBatch(batch [][]interface{}) error {
	db := w.db.Load()
	if db == nil {
		return fmt.Errorf("mod_log_mysql: not connected")
	}

	args := make([]interface{}, 0, len(batch)*len(Columns()))
	for _, row := range batch {
		args = append(args, row...)
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(w.conf.Writer.BatchTimeoutMs)*time.Millisecond)
	defer cancel()

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = txn.ExecContext(ctx, w.buildStmt(len(batch)), args...)
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
