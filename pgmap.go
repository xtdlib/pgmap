package pgmap

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xtdlib/rat"
)

// Store is the interface for basic key-value operations.
type Store[K comparable, V any] interface {
	TrySet(key K, value V, ttl ...time.Duration) (V, error)
	Set(key K, value V, ttl ...time.Duration) V
	TryGet(key K) (V, error)
	Get(key K) V
	TryGetOr(key K, defaultValue V) (V, error)
	GetOr(key K, defaultValue V) V
	TryHas(key K) (bool, error)
	Has(key K) bool
	TryDelete(key K) error
	Delete(key K)
	Keys(yield func(K) bool)
	KeysBackward(yield func(K) bool)
	All(yield func(K, V) bool)
	KeysWhere(whereClause string) func(yield func(K) bool)
	AllWhere(whereClause string) func(yield func(K, V) bool)
	Map() map[K]V
}

// MapStore extends Store with Map-specific operations.
type MapStore[K comparable, V any] interface {
	Store[K, V]
	TryClear() error
	Clear()
	TryPurge() error
	Purge()
	TrySetNX(key K, value V) (bool, error)
	SetNX(key K, value V) bool
	SetNZ(key K, value V) V
	Update(key K, fn func(V) V) (V, error)
	TryAddRat(key K, delta any) (*rat.Rational, error)
	AddRat(key K, delta any) *rat.Rational
	TryWithLock(mode LockMode, fn func(tx *Tx[K, V]) error) error
	WithLock(mode LockMode, fn func(tx *Tx[K, V]) error)
}

type LockMode int

const (
	LockShare LockMode = iota
	LockExclusive
	LockAccessExclusive
)

func (m LockMode) String() string {
	switch m {
	case LockShare:
		return "SHARE"
	case LockExclusive:
		return "EXCLUSIVE"
	case LockAccessExclusive:
		return "ACCESS EXCLUSIVE"
	default:
		return "SHARE"
	}
}

type Option func(*config)

type config struct {
	dsn        string
	syncCommit bool
	unlogged   bool
	minConns   int32
}

func DSN(dsn string) Option {
	return func(c *config) { c.dsn = dsn }
}

// SyncCommit enables synchronous_commit (WAL flush on every write).
// By default, pgmap uses asynchronous commit for faster writes.
func SyncCommit() Option {
	return func(c *config) { c.syncCommit = true }
}

// Unlogged creates an unlogged table (no WAL). Much faster writes but
// data is lost on crash or restart.
func Unlogged() Option {
	return func(c *config) { c.unlogged = true }
}

// MinConns sets the minimum number of idle connections in the pool.
func MinConns(n int32) Option {
	return func(c *config) { c.minConns = n }
}

// defaultDSN returns a unix socket DSN if a PostgreSQL socket is found,
// otherwise falls back to TCP localhost.
func defaultDSN() string {
	socketDirs := []string{
		"/var/run/postgresql",
		"/tmp",
		"/run/postgresql",
	}
	for _, dir := range socketDirs {
		sock := filepath.Join(dir, ".s.PGSQL.5432")
		if conn, err := net.Dial("unix", sock); err == nil {
			conn.Close()
			return fmt.Sprintf("host=%s user=postgres dbname=postgres", dir)
		}
	}
	return "postgres://postgres:postgres@localhost:5432/postgres"
}

type colType struct {
	pgType       string
	needsMarshal bool
	isScanner    bool         // implements sql.Scanner + fmt.Stringer
	goType       reflect.Type // cached reflect.Type for unmarshal
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type base[K comparable, V any] struct {
	table   string
	pool    *pgxpool.Pool
	q       querier
	keyType colType
	valType colType
	qSet    string
	qSetTTL string
	qGet    string
	qHas    string
	qDel    string
	qKeys   string
	qKeysB  string
	qAll    string
}

type Map[K comparable, V any] struct {
	base[K, V]
	unlogged bool
	qClear   string
	qPurge   string
	qSetNX   string
	qUpdate  string
}

type Tx[K comparable, V any] struct {
	base[K, V]
	tx pgx.Tx
}

// bg is a shared background context. Timeouts are enforced by
// PostgreSQL's statement_timeout (set at session level in AfterConnect).
var bg = context.Background()

func detectColType(v any) colType {
	if v == nil {
		return colType{pgType: "JSON", needsMarshal: true}
	}
	t := reflect.TypeOf(v)
	switch t.Kind() {
	case reflect.String:
		return colType{pgType: "TEXT", needsMarshal: false, goType: t}
	case reflect.Bool:
		return colType{pgType: "BOOLEAN", needsMarshal: false, goType: t}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return colType{pgType: "BIGINT", needsMarshal: false, goType: t}
	case reflect.Float32, reflect.Float64:
		return colType{pgType: "DOUBLE PRECISION", needsMarshal: false, goType: t}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return colType{pgType: "BYTEA", needsMarshal: false, goType: t}
		}
		return colType{pgType: "JSON", needsMarshal: true, goType: t}
	case reflect.Ptr:
		if t == reflect.TypeOf((*time.Time)(nil)) {
			return colType{pgType: "BIGINT", needsMarshal: false, goType: t}
		}
	}
	switch v.(type) {
	case time.Time:
		return colType{pgType: "BIGINT", needsMarshal: false, goType: t}
	}
	// Check if implements both sql.Scanner and fmt.Stringer
	scannerType := reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	stringerType := reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	ptrType := reflect.PointerTo(t)
	if ptrType.Implements(scannerType) && (t.Implements(stringerType) || ptrType.Implements(stringerType)) {
		return colType{pgType: "TEXT", needsMarshal: true, isScanner: true, goType: t}
	}
	return colType{pgType: "JSON", needsMarshal: true, goType: t}
}

func TryNew[K comparable, V any](tableName string, opts ...Option) (*Map[K, V], error) {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.dsn == "" {
		cfg.dsn = os.Getenv("PGMAP_DSN")
	}
	if cfg.dsn == "" {
		cfg.dsn = defaultDSN()
	}

	tableName = strings.ReplaceAll(tableName, "-", "_")

	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("pgmap: parse dsn: %w", err)
	}
	minConns := int32(3)
	if cfg.minConns > 0 {
		minConns = cfg.minConns
	}
	poolCfg.MinConns = minConns
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// 60s statement_timeout as safety net; regular ops finish in <1ms
		_, err := conn.Exec(ctx, "SET statement_timeout = '120s'")
		if err != nil {
			return err
		}
		if !cfg.syncCommit {
			_, err = conn.Exec(ctx, "SET synchronous_commit = off")
		}
		return err
	}

	pool, err := pgxpool.NewWithConfig(bg, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgmap: connect: %w", err)
	}

	var zeroK K
	var zeroV V
	keyType := detectColType(zeroK)
	valType := detectColType(zeroV)

	m := &Map[K, V]{
		base: base[K, V]{
			table:   tableName,
			pool:    pool,
			keyType: keyType,
			valType: valType,
		},
		unlogged: cfg.unlogged,
	}
	m.q = pool

	if err := m.ensureTable(); err != nil {
		pool.Close()
		return nil, err
	}

	m.buildQueries()
	go m.cleanupLoop()

	return m, nil
}

func New[K comparable, V any](tableName string, opts ...Option) *Map[K, V] {
	m, err := TryNew[K, V](tableName, opts...)
	if err != nil {
		panic(err)
	}
	return m
}

func (m *Map[K, V]) ensureTable() error {
	// Check if table exists and has correct schema
	var exists bool
	err := m.pool.QueryRow(bg, `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, m.table).Scan(&exists)
	if err != nil {
		return fmt.Errorf("pgmap: check table: %w", err)
	}

	if exists {
		// Verify column types, drop and recreate if wrong
		var keyPgType, valPgType string
		err = m.pool.QueryRow(bg, `
			SELECT
				(SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = 'key'),
				(SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = 'value')
		`, m.table).Scan(&keyPgType, &valPgType)
		if err != nil {
			return fmt.Errorf("pgmap: check columns: %w", err)
		}

		needsRecreate := !matchesPgType(keyPgType, m.keyType.pgType) || !matchesPgType(valPgType, m.valType.pgType)
		if needsRecreate {
			_, err = m.pool.Exec(bg, fmt.Sprintf(`DROP TABLE %s`, m.table))
			if err != nil {
				return fmt.Errorf("pgmap: drop table: %w", err)
			}
			exists = false
		} else {
			// Check for expires_at column
			var hasExpires bool
			err = m.pool.QueryRow(bg, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = 'expires_at')`, m.table).Scan(&hasExpires)
			if err != nil {
				return fmt.Errorf("pgmap: check expires_at: %w", err)
			}
			if !hasExpires {
				_, err = m.pool.Exec(bg, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN expires_at TIMESTAMPTZ`, m.table))
				if err != nil {
					return fmt.Errorf("pgmap: add expires_at: %w", err)
				}
			}
		}
	}

	if !exists {
		unlogged := ""
		if m.unlogged {
			unlogged = "UNLOGGED"
		}
		createSQL := fmt.Sprintf(`CREATE %s TABLE IF NOT EXISTS %s (key %s PRIMARY KEY, value %s NOT NULL, expires_at TIMESTAMPTZ)`,
			unlogged, m.table, m.keyType.pgType, m.valType.pgType)
		_, err = m.pool.Exec(bg, createSQL)
		if err != nil {
			return fmt.Errorf("pgmap: create table: %w", err)
		}
	}

	return nil
}

func matchesPgType(actual, expected string) bool {
	actual = strings.ToUpper(actual)
	expected = strings.ToUpper(expected)
	if actual == expected {
		return true
	}
	// Handle PostgreSQL type aliases
	aliases := map[string][]string{
		"BIGINT":           {"BIGINT", "INT8"},
		"DOUBLE PRECISION": {"DOUBLE PRECISION", "FLOAT8"},
		"TEXT":             {"TEXT", "CHARACTER VARYING"},
		"BOOLEAN":          {"BOOLEAN", "BOOL"},
		"BYTEA":            {"BYTEA"},
		"JSON":             {"JSON", "JSONB"},
	}
	for _, alts := range aliases {
		inActual, inExpected := false, false
		for _, a := range alts {
			if actual == a {
				inActual = true
			}
			if expected == a {
				inExpected = true
			}
		}
		if inActual && inExpected {
			return true
		}
	}
	return false
}

func (m *Map[K, V]) buildQueries() {
	notExpired := "(expires_at IS NULL OR expires_at > NOW())"
	m.qSet = fmt.Sprintf(`INSERT INTO %s (key, value, expires_at) VALUES ($1, $2, NULL) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL`, m.table)
	m.qSetTTL = fmt.Sprintf(`INSERT INTO %s (key, value, expires_at) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at`, m.table)
	m.qGet = fmt.Sprintf(`SELECT value FROM %s WHERE key = $1 AND %s FOR UPDATE`, m.table, notExpired)
	m.qHas = fmt.Sprintf(`SELECT 1 FROM %s WHERE key = $1 AND %s LIMIT 1`, m.table, notExpired)
	m.qDel = fmt.Sprintf(`DELETE FROM %s WHERE key = $1`, m.table)
	m.qKeys = fmt.Sprintf(`SELECT key FROM %s WHERE %s ORDER BY key`, m.table, notExpired)
	m.qKeysB = fmt.Sprintf(`SELECT key FROM %s WHERE %s ORDER BY key DESC`, m.table, notExpired)
	m.qAll = fmt.Sprintf(`SELECT key, value FROM %s WHERE %s ORDER BY key`, m.table, notExpired)
	m.qClear = fmt.Sprintf(`DELETE FROM %s`, m.table)
	m.qPurge = fmt.Sprintf(`DROP TABLE IF EXISTS %s`, m.table)
	m.qSetNX = fmt.Sprintf(`INSERT INTO %s (key, value, expires_at) VALUES ($1, $2, NULL) ON CONFLICT (key) DO NOTHING`, m.table)
	m.qUpdate = fmt.Sprintf(`SELECT value FROM %s WHERE key = $1 AND %s FOR UPDATE`, m.table, notExpired)
}

func (m *Map[K, V]) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		m.pool.Exec(bg, fmt.Sprintf(`DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at <= NOW()`, m.table))
	}
}

// marshalKey converts a Go key to its database representation
func (b *base[K, V]) marshalKey(key K) any {
	if !b.keyType.needsMarshal {
		// Handle time.Time specially
		if t, ok := any(key).(time.Time); ok {
			return t.UnixNano()
		}
		if tp, ok := any(key).(*time.Time); ok {
			if tp == nil {
				return int64(0)
			}
			return tp.UnixNano()
		}
		return key
	}
	if b.keyType.isScanner {
		if s, ok := any(key).(fmt.Stringer); ok {
			return s.String()
		}
		return fmt.Sprint(key)
	}
	data, _ := json.Marshal(key)
	return string(data)
}

// marshalValue converts a Go value to its database representation
func (b *base[K, V]) marshalValue(value V) any {
	if !b.valType.needsMarshal {
		if t, ok := any(value).(time.Time); ok {
			return t.UnixNano()
		}
		if tp, ok := any(value).(*time.Time); ok {
			if tp == nil {
				return int64(0)
			}
			return tp.UnixNano()
		}
		return value
	}
	if b.valType.isScanner {
		if s, ok := any(value).(fmt.Stringer); ok {
			return s.String()
		}
		return fmt.Sprint(value)
	}
	data, _ := json.Marshal(value)
	return string(data)
}

// unmarshalKey converts database representation back to Go key
func (b *base[K, V]) unmarshalKey(src any) (K, error) {
	var zero K
	if !b.keyType.needsMarshal {
		return convertToType[K](src, b.keyType)
	}
	if b.keyType.isScanner {
		s, ok := src.(string)
		if !ok {
			return zero, fmt.Errorf("expected string for scanner type")
		}
		ptr := reflect.New(b.keyType.goType)
		if scanner, ok := ptr.Interface().(sql.Scanner); ok {
			if err := scanner.Scan(s); err != nil {
				return zero, err
			}
			return ptr.Elem().Interface().(K), nil
		}
	}
	var data []byte
	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return zero, fmt.Errorf("expected string for json type, got %T", src)
	}
	var result K
	if err := json.Unmarshal(data, &result); err != nil {
		return zero, err
	}
	return result, nil
}

// unmarshalValue converts database representation back to Go value
func (b *base[K, V]) unmarshalValue(src any) (V, error) {
	var zero V
	if !b.valType.needsMarshal {
		return convertToType[V](src, b.valType)
	}
	if b.valType.isScanner {
		s, ok := src.(string)
		if !ok {
			return zero, fmt.Errorf("expected string for scanner type")
		}
		ptr := reflect.New(b.valType.goType)
		if scanner, ok := ptr.Interface().(sql.Scanner); ok {
			if err := scanner.Scan(s); err != nil {
				return zero, err
			}
			return ptr.Elem().Interface().(V), nil
		}
	}
	var data []byte
	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		// pgx already unmarshaled JSON — re-marshal to get raw bytes
		var err error
		data, err = json.Marshal(src)
		if err != nil {
			return zero, err
		}
	}
	var result V
	if err := json.Unmarshal(data, &result); err != nil {
		return zero, err
	}
	return result, nil
}

var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf((*time.Time)(nil))

func convertToType[T any](src any, ct colType) (T, error) {
	var zero T

	// Handle time.Time stored as int64
	if ct.goType == timeType {
		n, ok := toInt64(src)
		if !ok {
			return zero, fmt.Errorf("cannot convert %T to int64 for time", src)
		}
		t := time.Unix(0, n)
		return any(t).(T), nil
	}
	if ct.goType == timePtrType {
		n, ok := toInt64(src)
		if !ok {
			return zero, fmt.Errorf("cannot convert %T to int64 for *time", src)
		}
		t := time.Unix(0, n)
		return any(&t).(T), nil
	}

	// Handle numeric types
	switch any(zero).(type) {
	case int:
		n, _ := toInt64(src)
		return any(int(n)).(T), nil
	case int8:
		n, _ := toInt64(src)
		return any(int8(n)).(T), nil
	case int16:
		n, _ := toInt64(src)
		return any(int16(n)).(T), nil
	case int32:
		n, _ := toInt64(src)
		return any(int32(n)).(T), nil
	case int64:
		n, _ := toInt64(src)
		return any(n).(T), nil
	case uint:
		n, _ := toInt64(src)
		return any(uint(n)).(T), nil
	case uint8:
		n, _ := toInt64(src)
		return any(uint8(n)).(T), nil
	case uint16:
		n, _ := toInt64(src)
		return any(uint16(n)).(T), nil
	case uint32:
		n, _ := toInt64(src)
		return any(uint32(n)).(T), nil
	case uint64:
		n, _ := toInt64(src)
		return any(uint64(n)).(T), nil
	case float32:
		f, _ := toFloat64(src)
		return any(float32(f)).(T), nil
	case float64:
		f, _ := toFloat64(src)
		return any(f).(T), nil
	case bool:
		b, _ := src.(bool)
		return any(b).(T), nil
	case string:
		s, _ := src.(string)
		return any(s).(T), nil
	case []byte:
		switch v := src.(type) {
		case []byte:
			return any(v).(T), nil
		case string:
			return any([]byte(v)).(T), nil
		}
	}

	// Fallback: try direct type assertion
	if v, ok := src.(T); ok {
		return v, nil
	}
	return zero, fmt.Errorf("cannot convert %T to %T", src, zero)
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// TrySet sets a key-value pair with optional TTL, returns the new value
func (b *base[K, V]) TrySet(key K, value V, ttl ...time.Duration) (V, error) {
	dbKey := b.marshalKey(key)
	dbVal := b.marshalValue(value)

	var err error
	if len(ttl) > 0 && ttl[0] > 0 {
		_, err = b.q.Exec(bg, b.qSetTTL, dbKey, dbVal, time.Now().Add(ttl[0]))
	} else {
		_, err = b.q.Exec(bg, b.qSet, dbKey, dbVal)
	}
	if err != nil {
		var zero V
		return zero, fmt.Errorf("pgmap: set: %w", err)
	}

	return value, nil
}

func (b *base[K, V]) Set(key K, value V, ttl ...time.Duration) V {
	v, err := b.TrySet(key, value, ttl...)
	if err != nil {
		panic(err)
	}
	return v
}

// TryGet retrieves a value by key. Uses FOR UPDATE to block concurrent writers.
func (b *base[K, V]) TryGet(key K) (V, error) {
	var zero V
	dbKey := b.marshalKey(key)

	row := b.q.QueryRow(bg, b.qGet, dbKey)
	var rawVal any
	if err := row.Scan(&rawVal); err != nil {
		return zero, err
	}

	return b.unmarshalValue(rawVal)
}

func (b *base[K, V]) Get(key K) V {
	v, err := b.TryGet(key)
	if err != nil {
		panic(err)
	}
	return v
}

// TryGetOr returns the value or a default if not found
func (b *base[K, V]) TryGetOr(key K, defaultValue V) (V, error) {
	v, err := b.TryGet(key)
	if errors.Is(err, pgx.ErrNoRows) {
		return defaultValue, nil
	}
	if err != nil {
		return defaultValue, err
	}
	return v, nil
}

func (b *base[K, V]) GetOr(key K, defaultValue V) V {
	v, err := b.TryGetOr(key, defaultValue)
	if err != nil {
		panic(err)
	}
	return v
}

// TryHas checks if a key exists
func (b *base[K, V]) TryHas(key K) (bool, error) {
	dbKey := b.marshalKey(key)
	row := b.q.QueryRow(bg, b.qHas, dbKey)
	var dummy int
	if err := row.Scan(&dummy); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *base[K, V]) Has(key K) bool {
	v, err := b.TryHas(key)
	if err != nil {
		panic(err)
	}
	return v
}

// TryDelete removes a key
func (b *base[K, V]) TryDelete(key K) error {
	dbKey := b.marshalKey(key)
	_, err := b.q.Exec(bg, b.qDel, dbKey)
	return err
}

func (b *base[K, V]) Delete(key K) {
	if err := b.TryDelete(key); err != nil {
		panic(err)
	}
}

// Keys iterates over all keys in order
func (b *base[K, V]) Keys(yield func(K) bool) {
	b.keysQuery(b.qKeys, yield)
}

// KeysBackward iterates over all keys in reverse order
func (b *base[K, V]) KeysBackward(yield func(K) bool) {
	b.keysQuery(b.qKeysB, yield)
}

func (b *base[K, V]) keysQuery(query string, yield func(K) bool) {
	rows, err := b.q.Query(bg, query)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var rawKey any
		if err := rows.Scan(&rawKey); err != nil {
			return
		}
		key, err := b.unmarshalKey(rawKey)
		if err != nil {
			return
		}
		if !yield(key) {
			return
		}
	}
}

// KeysWhere returns an iterator with a custom WHERE clause
func (b *base[K, V]) KeysWhere(whereClause string) func(yield func(K) bool) {
	return func(yield func(K) bool) {
		query := fmt.Sprintf(`SELECT key FROM %s WHERE (expires_at IS NULL OR expires_at > NOW()) AND (%s) ORDER BY key`, b.table, whereClause)
		b.keysQuery(query, yield)
	}
}

// All iterates over all key-value pairs
func (b *base[K, V]) All(yield func(K, V) bool) {
	b.allQuery(b.qAll, yield)
}

func (b *base[K, V]) allQuery(query string, yield func(K, V) bool) {
	rows, err := b.q.Query(bg, query)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var rawKey, rawVal any
		if err := rows.Scan(&rawKey, &rawVal); err != nil {
			return
		}
		key, err := b.unmarshalKey(rawKey)
		if err != nil {
			return
		}
		val, err := b.unmarshalValue(rawVal)
		if err != nil {
			return
		}
		if !yield(key, val) {
			return
		}
	}
}

// AllWhere returns an iterator with a custom WHERE clause
func (b *base[K, V]) AllWhere(whereClause string) func(yield func(K, V) bool) {
	return func(yield func(K, V) bool) {
		query := fmt.Sprintf(`SELECT key, value FROM %s WHERE (expires_at IS NULL OR expires_at > NOW()) AND (%s) ORDER BY key`, b.table, whereClause)
		b.allQuery(query, yield)
	}
}

// Map returns all key-value pairs as a Go map
func (b *base[K, V]) Map() map[K]V {
	result := make(map[K]V)
	b.All(func(k K, v V) bool {
		result[k] = v
		return true
	})
	return result
}

// TryClear removes all entries
func (m *Map[K, V]) TryClear() error {
	_, err := m.pool.Exec(bg, m.qClear)
	return err
}

func (m *Map[K, V]) Clear() {
	if err := m.TryClear(); err != nil {
		panic(err)
	}
}

// TryPurge drops the table
func (m *Map[K, V]) TryPurge() error {
	_, err := m.pool.Exec(bg, m.qPurge)
	return err
}

func (m *Map[K, V]) Purge() {
	if err := m.TryPurge(); err != nil {
		panic(err)
	}
}

// TrySetNX sets a value only if the key doesn't exist
func (m *Map[K, V]) TrySetNX(key K, value V) (bool, error) {
	dbKey := m.marshalKey(key)
	dbVal := m.marshalValue(value)

	tag, err := m.pool.Exec(bg, m.qSetNX, dbKey, dbVal)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (m *Map[K, V]) SetNX(key K, value V) bool {
	v, err := m.TrySetNX(key, value)
	if err != nil {
		panic(err)
	}
	return v
}

// SetNZ sets a value only if it's non-zero
func (m *Map[K, V]) SetNZ(key K, value V) V {
	var zero V
	if cmp.Equal(value, zero) {
		return m.GetOr(key, zero)
	}
	return m.Set(key, value)
}

// Update atomically updates a value. Retries on conflict when the row
// doesn't exist yet (FOR UPDATE can't lock a non-existent row).
func (m *Map[K, V]) Update(key K, fn func(V) V) (V, error) {
	dbKey := m.marshalKey(key)

	for {
		result, err := m.tryUpdate(dbKey, fn)
		if err != nil {
			// Retry on serialization failure or deadlock
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
				continue
			}
			return result, err
		}
		return result, nil
	}
}

func (m *Map[K, V]) tryUpdate(dbKey any, fn func(V) V) (V, error) {
	tx, err := m.pool.Begin(bg)
	if err != nil {
		var zero V
		return zero, err
	}
	defer tx.Rollback(bg)

	// Ensure row exists so FOR UPDATE can lock it.
	var zeroV V
	dbZero := m.marshalValue(zeroV)
	_, err = tx.Exec(bg, m.qSetNX, dbKey, dbZero)
	if err != nil {
		var zero V
		return zero, err
	}

	// Now SELECT FOR UPDATE always finds a row to lock
	var rawVal any
	var current V
	err = tx.QueryRow(bg, m.qUpdate, dbKey).Scan(&rawVal)
	if err != nil {
		var zero V
		return zero, err
	}
	current, err = m.unmarshalValue(rawVal)
	if err != nil {
		var zero V
		return zero, err
	}

	newVal := fn(current)
	dbVal := m.marshalValue(newVal)

	_, err = tx.Exec(bg, m.qSet, dbKey, dbVal)
	if err != nil {
		var zero V
		return zero, err
	}

	if err := tx.Commit(bg); err != nil {
		var zero V
		return zero, err
	}

	return newVal, nil
}

// TryAddRat atomically adds a rational number
func (m *Map[K, V]) TryAddRat(key K, delta any) (*rat.Rational, error) {
	var result *rat.Rational
	_, err := m.Update(key, func(v V) V {
		current := rat.Rat(v)
		if current == nil {
			current = rat.Rat(0)
		}
		result = current.Add(delta)
		// Convert back to V
		return any(result).(V)
	})
	return result, err
}

func (m *Map[K, V]) AddRat(key K, delta any) *rat.Rational {
	r, err := m.TryAddRat(key, delta)
	if err != nil {
		panic(err)
	}
	return r
}

// TryWithLock executes a function with table-level lock
func (m *Map[K, V]) TryWithLock(mode LockMode, fn func(tx *Tx[K, V]) error) error {
	pgxTx, err := m.pool.Begin(bg)
	if err != nil {
		return err
	}
	defer pgxTx.Rollback(bg)

	lockSQL := fmt.Sprintf(`LOCK TABLE %s IN %s MODE`, m.table, mode.String())
	if _, err := pgxTx.Exec(bg, lockSQL); err != nil {
		return err
	}

	tx := &Tx[K, V]{
		base: base[K, V]{
			table:   m.table,
			pool:    m.pool,
			q:       pgxTx,
			keyType: m.keyType,
			valType: m.valType,
			qSet:    m.qSet,
			qSetTTL: m.qSetTTL,
			qGet:    m.qGet,
			qHas:    m.qHas,
			qDel:    m.qDel,
			qKeys:   m.qKeys,
			qKeysB:  m.qKeysB,
			qAll:    m.qAll,
		},
		tx: pgxTx,
	}

	if err := fn(tx); err != nil {
		return err
	}

	return pgxTx.Commit(bg)
}

func (m *Map[K, V]) WithLock(mode LockMode, fn func(tx *Tx[K, V]) error) {
	if err := m.TryWithLock(mode, fn); err != nil {
		panic(err)
	}
}

// Close closes the underlying connection pool
func (m *Map[K, V]) Close() {
	m.pool.Close()
}

// Compile-time interface checks
var _ Store[string, string] = (*base[string, string])(nil)
var _ MapStore[string, string] = (*Map[string, string])(nil)
