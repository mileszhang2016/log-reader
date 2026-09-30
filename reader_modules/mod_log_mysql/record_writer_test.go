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
	"errors"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
)

func newTestConf() *ConfModLogMysql {
	return &ConfModLogMysql{
		Mysql: ConfMysql{
			Addr:                   "127.0.0.1:3306",
			User:                   "report",
			DBName:                 "bfe_report",
			Table:                  "test_table",
			ConnectTimeoutMs:       3000,
			ConnectRetryIntervalMs: 50, // fast retry in tests
		},
		Writer: ConfLogMysqlWriter{
			QueueSize:       16,
			BatchSize:       3,
			FlushIntervalMs: 60000, // long interval: count-based flush in most tests
			MaxRetries:      1,
			MaxOpenConns:    2,
			MaxIdleConns:    1,
		},
	}
}

func newTestState() *module_state2.State {
	state := &module_state2.State{}
	state.Init()
	state.CountersInit(COUNTER_KEYS)
	return state
}

// newTestWriterWithConf creates a writer wired to a sqlmock db: the connector
// seam is replaced so Connect() returns the mock db directly (ping assumed).
// Connect() is already called; the writer is not started.
func newTestWriterWithConf(t *testing.T, conf *ConfModLogMysql) (*RecordWriter, sqlmock.Sqlmock, *module_state2.State) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) { return db, nil }
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	return w, mock, state
}

func newTestWriter(t *testing.T) (*RecordWriter, sqlmock.Sqlmock, *module_state2.State) {
	t.Helper()
	return newTestWriterWithConf(t, newTestConf())
}

// makeRow returns a row with one value per column.
func makeRow() []interface{} {
	return make([]interface{}, len(Columns()))
}

// expectedInsertSQL builds the expected INSERT statement independently from the
// DDL column list hardcoded in field_mapper_test.go.
func expectedInsertSQL(table string, cols []string, n int) string {
	row := "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
	rows := make([]string, n)
	for i := range rows {
		rows[i] = row
	}

	updates := make([]string, len(cols))
	for i, c := range cols {
		updates[i] = c + "=VALUES(" + c + ")"
	}

	return "INSERT INTO " + table + " (" + strings.Join(cols, ",") + ") VALUES " +
		strings.Join(rows, ",") + " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
}

func waitForCounter(t *testing.T, state *module_state2.State, key string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if state.GetCounter(key) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting counter %s == %d, current %d", key, want, state.GetCounter(key))
}

func TestRecordWriter_BatchByCount(t *testing.T) {
	w, mock, state := newTestWriter(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 3))).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()

	w.Start()

	for i := 0; i < 3; i++ {
		if !w.Enqueue(makeRow()) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 3, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_BatchByTimeout(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 100
	conf.Writer.FlushIntervalMs = 50
	w, mock, state := newTestWriterWithConf(t, conf)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 1))).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	w.Start()

	if !w.Enqueue(makeRow()) {
		t.Fatal("Enqueue failed")
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 1, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_SQLShape(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 2
	w, mock, state := newTestWriterWithConf(t, conf)

	// exact statement: placeholder count == 2 * 89, ON DUPLICATE KEY UPDATE covers all columns
	stmt := w.buildStmt(2)
	expected := expectedInsertSQL("test_table", ddlColumns, 2)
	if stmt != expected {
		t.Fatalf("buildStmt mismatch:\n got: %s\nwant: %s", stmt, expected)
	}
	if strings.Count(stmt, "?") != 2*len(ddlColumns) {
		t.Errorf("placeholder count = %d, want %d", strings.Count(stmt, "?"), 2*len(ddlColumns))
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expected)).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	w.Start()
	w.Enqueue(makeRow())
	w.Enqueue(makeRow())

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 2, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_RetryThenSuccess(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1 // flush on every enqueue
	w, mock, state := newTestWriterWithConf(t, conf)

	// first attempt fails, retry (after 200ms backoff) succeeds
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("connection lost"))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO test_table").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	w.Start()
	w.Enqueue(makeRow())

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 1, 5*time.Second)
	w.Close()

	if got := state.GetCounter("SEND_MYSQL_FAILED"); got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_RetryExhausted(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1
	conf.Writer.MaxRetries = 2 // initial attempt + 2 retries (200ms + 400ms backoff)
	w, mock, state := newTestWriterWithConf(t, conf)

	for i := 0; i < 3; i++ {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("db down"))
		mock.ExpectRollback()
	}

	w.Start()
	w.Enqueue(makeRow())

	waitForCounter(t, state, "SEND_MYSQL_FAILED", 1, 5*time.Second)
	w.Close()

	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_DrainOnClose(t *testing.T) {
	w, mock, state := newTestWriter(t)

	// batch not full (2 < 3), Close must drain and flush the residual rows
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 2))).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	w.Start()
	w.Enqueue(makeRow())
	w.Enqueue(makeRow())
	w.Close() // drain + final flush happens before Close returns

	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 2 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 2 after drain", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_EnqueueFull(t *testing.T) {
	conf := newTestConf()
	conf.Writer.QueueSize = 1
	w := NewRecordWriter(conf, newTestState()) // not started: channel never drains

	if !w.Enqueue(makeRow()) {
		t.Error("first Enqueue should succeed")
	}
	if w.Enqueue(makeRow()) {
		t.Error("second Enqueue should fail when channel full")
	}
	w.Close()
}

// TestRecordWriter_ConnectIdempotent: a second Connect on an already connected
// writer must be a no-op (the connector is not invoked again).
func TestRecordWriter_ConnectIdempotent(t *testing.T) {
	var calls int32
	w := NewRecordWriter(newTestConf(), newTestState())
	// fail twice, then swap in a fake db
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		if atomic.AddInt32(&calls, 1) <= 2 {
			return nil, errors.New("mysql down")
		}
		db, _, err := sqlmock.New()
		return db, err
	}
	if err := w.Connect(); err == nil {
		t.Fatal("first Connect should fail")
	}
	if err := w.Connect(); err == nil {
		t.Fatal("second Connect should fail")
	}
	if err := w.Connect(); err != nil {
		t.Fatalf("third Connect should succeed: %v", err)
	}
	if !w.Connected() {
		t.Fatal("Connected() should be true")
	}
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect on connected writer should be no-op: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("connector called %d times, want 3 (no-op on connected)", got)
	}
}

// TestRecordWriter_BufferThenConnectDrain: while mysql is unreachable the
// writeLoop must not consume the queue (rows accumulate in ch); after the
// retry loop connects, the backlog drains in batches as before.
func TestRecordWriter_BufferThenConnectDrain(t *testing.T) {
	conf := newTestConf()
	conf.Writer.FlushIntervalMs = 50 // ticker-based flush after connect

	var failing atomic.Bool
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		if failing.Load() {
			return nil, errors.New("mysql down")
		}
		return db, nil
	}

	failing.Store(true)
	if err := w.Connect(); err == nil {
		t.Fatal("initial Connect should fail while failing is set")
	}
	w.Start() // writeLoop parks on connCh; connectLoop retries every 50ms

	// rows enqueue into the buffered queue while disconnected
	for i := 0; i < 2; i++ {
		if !w.Enqueue(makeRow()) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}

	// retry loop is running; nothing is written yet
	waitForCounter(t, state, "MYSQL_CONN_RETRY", 2, 5*time.Second)
	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Fatalf("WRITE_BATCH_SIZE = %d, want 0 while disconnected", got)
	}

	// expect a single 2-row batch for the buffered backlog
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 2))).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	failing.Store(false)
	waitForCounter(t, state, "MYSQL_CONN_OK", 1, 5*time.Second)
	if !w.Connected() {
		t.Fatal("Connected() should be true after retry")
	}
	if got := state.GetState("MYSQL_CONN_STATE"); got != "UP" {
		t.Errorf("MYSQL_CONN_STATE = %q, want UP", got)
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 2, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// TestRecordWriter_CloseBeforeConnected: Close while the retry loop is still
// running must shut down both goroutines promptly and drop buffered rows.
func TestRecordWriter_CloseBeforeConnected(t *testing.T) {
	conf := newTestConf()
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		return nil, errors.New("mysql down")
	}

	w.Start()
	w.Enqueue(makeRow())
	w.Enqueue(makeRow())

	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked with unconnected writer")
	}

	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 0 (buffered rows dropped)", got)
	}
	if got := state.GetCounter("MYSQL_CONN_OK"); got != 0 {
		t.Errorf("MYSQL_CONN_OK = %d, want 0", got)
	}
}
