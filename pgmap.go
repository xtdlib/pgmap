package pgmap

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xtdlib/pgx/pgxpool"
	"github.com/xtdlib/rat"
)

// executor is the common interface for pgxpool.Pool and pgx.Tx
type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// base contains common fields and operations for Map and Tx
type base[K comparable, V any] struct {
	db                executor
	tableName         string
	keyNeedsMarshal   bool
	valueNeedsMarshal bool
}

type Map[K comparable, V any] struct {
	base[K, V]
	pool *pgxpool.Pool // keep pool reference for Begin()
}

// Option is a functional option for configuring KV
type Option func(*config)

type config struct {
	dsn       string
	tableName string
}

// DSN sets the PostgreSQL connection string
func DSN(dsn string) Option {
	return func(c *config) {
		c.dsn = dsn
	}
}

// TryNew creates a new KV store with required table name and optional configuration.
// Uses pgmap_DSN env var or defaults to localhost postgres if DSN not provided.
func TryNew[K comparable, V any](tableName string, opts ...Option) (*Map[K, V], error) {
	tableName = strings.ReplaceAll(tableName, "-", "_")

	cfg := &config{
		tableName: tableName,
	}

	// Apply options
	for _, opt := range opts {
		opt(cfg)
	}

	// Resolve DSN
	dsn := cfg.dsn
	if dsn == "" && os.Getenv("PGMAP_DSN") != "" {
		dsn = os.Getenv("PGMAP_DSN")
	}
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres"
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, err
	}

	kv := &Map[K, V]{
		base: base[K, V]{
			db:        pool,
			tableName: cfg.tableName,
		},
		pool: pool,
	}

	var k K
	keyType := "JSON"
	kv.keyNeedsMarshal = true // default to true for JSON type

	// Check if type implements both sql.Scanner and fmt.Stringer - if so, use TEXT
	_, implementsScanner := any(k).(sql.Scanner)
	_, implementsStringer := any(k).(fmt.Stringer)
	if !implementsScanner || !implementsStringer {
		rv := reflect.ValueOf(k)
		if rv.Kind() == reflect.Ptr {
			// For pointer types, create an instance to check
			if rv.IsNil() && rv.Type().Elem().Kind() != reflect.Interface {
				newVal := reflect.New(rv.Type().Elem())
				if !implementsScanner {
					_, implementsScanner = newVal.Interface().(sql.Scanner)
				}
				if !implementsStringer {
					_, implementsStringer = newVal.Interface().(fmt.Stringer)
				}
			}
		}
	}

	if implementsScanner && implementsStringer {
		keyType = "TEXT"
		kv.keyNeedsMarshal = true // Still needs marshal, but will use String() method
	}

	switch any(k).(type) {
	case string:
		keyType = "TEXT"
		kv.keyNeedsMarshal = false
	case []byte:
		keyType = "BYTEA"
		kv.keyNeedsMarshal = false
	case bool:
		keyType = "BOOLEAN"
		kv.keyNeedsMarshal = false
	case time.Time, *time.Time:
		keyType = "BIGINT"
		kv.keyNeedsMarshal = false
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		keyType = "BIGINT"
		kv.keyNeedsMarshal = false
	case float32, float64:
		keyType = "DOUBLE PRECISION"
		kv.keyNeedsMarshal = false
	}

	var v V
	valueType := "JSON"
	kv.valueNeedsMarshal = true // default to true for JSON type

	// Check if type implements both sql.Scanner and fmt.Stringer - if so, use TEXT
	_, valueImplementsScanner := any(v).(sql.Scanner)
	_, valueImplementsStringer := any(v).(fmt.Stringer)
	if !valueImplementsScanner || !valueImplementsStringer {
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr {
			// For pointer types, create an instance to check
			if rv.IsNil() && rv.Type().Elem().Kind() != reflect.Interface {
				newVal := reflect.New(rv.Type().Elem())
				if !valueImplementsScanner {
					_, valueImplementsScanner = newVal.Interface().(sql.Scanner)
				}
				if !valueImplementsStringer {
					_, valueImplementsStringer = newVal.Interface().(fmt.Stringer)
				}
			}
		}
	}

	if valueImplementsScanner && valueImplementsStringer {
		valueType = "TEXT"
		kv.valueNeedsMarshal = true // Still needs marshal, but will use String() method
	}

	switch any(v).(type) {
	case string:
		valueType = "TEXT"
		kv.valueNeedsMarshal = false
	case []byte:
		valueType = "BYTEA"
		kv.valueNeedsMarshal = false
	case bool:
		valueType = "BOOLEAN"
		kv.valueNeedsMarshal = false
	case time.Time, *time.Time:
		valueType = "BIGINT"
		kv.valueNeedsMarshal = false
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		valueType = "BIGINT"
		kv.valueNeedsMarshal = false
	case float32, float64:
		valueType = "DOUBLE PRECISION"
		kv.valueNeedsMarshal = false
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Check if table exists and has correct schema
	var existingKeyType, existingValueType string
	var hasExpiresAtColumn bool
	checkQuery := `
		SELECT
			pg_catalog.format_type(a1.atttypid, a1.atttypmod) as key_type,
			pg_catalog.format_type(a2.atttypid, a2.atttypmod) as value_type,
			EXISTS(SELECT 1 FROM pg_attribute a3 WHERE a3.attrelid = c.oid AND a3.attname = 'expires_at' AND a3.attnum > 0) as has_expires_at
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a1 ON a1.attrelid = c.oid AND a1.attname = 'key'
		JOIN pg_attribute a2 ON a2.attrelid = c.oid AND a2.attname = 'value'
		WHERE c.relname = $1 AND n.nspname = 'public'
	`
	err = kv.db.QueryRow(ctx, checkQuery, cfg.tableName).Scan(&existingKeyType, &existingValueType, &hasExpiresAtColumn)

	needsRecreate := false
	if err == nil {
		// Table exists, check if types match
		// Normalize type names for comparison
		normalizeType := func(t string) string {
			t = strings.ToLower(t)
			if strings.Contains(t, "bytea") {
				return "bytea"
			}
			if strings.Contains(t, "boolean") || t == "bool" {
				return "boolean"
			}
			if strings.Contains(t, "bigint") || t == "integer" {
				return "bigint"
			}
			if strings.Contains(t, "double") || strings.Contains(t, "precision") {
				return "double precision"
			}
			if strings.Contains(t, "json") {
				return "json"
			}
			if strings.Contains(t, "text") || strings.Contains(t, "character varying") {
				return "text"
			}
			return t
		}

		expectedKeyType := normalizeType(keyType)
		expectedValueType := normalizeType(valueType)
		actualKeyType := normalizeType(existingKeyType)
		actualValueType := normalizeType(existingValueType)

		if expectedKeyType != actualKeyType || expectedValueType != actualValueType {
			needsRecreate = true
			// Drop the existing table
			_, err = kv.db.Exec(ctx, `DROP TABLE IF EXISTS `+cfg.tableName)
			if err != nil {
				return nil, err
			}
		}
	} else if err != pgx.ErrNoRows {
		// Table doesn't exist, that's fine
		// Any other error should be handled
		needsRecreate = true
	}

	// Create table if it doesn't exist or was just dropped
	if needsRecreate || err == pgx.ErrNoRows {
		query := `
			CREATE TABLE IF NOT EXISTS ` + cfg.tableName + ` (
				key ` + keyType + ` PRIMARY KEY,
				value ` + valueType + ` NOT NULL,
				expires_at TIMESTAMPTZ
			)
		`
		_, err = kv.db.Exec(ctx, query)
		if err != nil {
			return nil, err
		}
	} else if !hasExpiresAtColumn {
		// Migrate existing table: add expires_at column
		_, err = kv.db.Exec(ctx, `ALTER TABLE `+cfg.tableName+` ADD COLUMN expires_at TIMESTAMPTZ`)
		if err != nil {
			return nil, err
		}
	}

	// Start background goroutine to clean up expired entries every 10 minutes
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			kv.db.Exec(ctx, `DELETE FROM `+cfg.tableName+` WHERE expires_at IS NOT NULL AND expires_at <= NOW()`)
			cancel()
		}
	}()

	return kv, nil
}

// New creates a new KV store with required table name and optional configuration, panics on error.
// Uses pgmap_DSN env var or defaults to localhost postgres if DSN not provided.
func New[K comparable, V any](tableName string, opts ...Option) *Map[K, V] {
	kv, err := TryNew[K, V](tableName, opts...)
	if err != nil {
		panic(err)
	}
	return kv
}

// func (kv *KV[K, V]) WithTransaction(fn func(pgx.Tx) error) error {
// 	tx, err := kv.db.Begin(context.Background())
// 	if err != nil {
// 		return err
// 	}
//
// 	defer tx.Rollback(context.Background())
//
// 	if err := fn(tx); err != nil {
// 		return err
// 	}
// 	return tx.Commit(context.Background())
// }

// TrySet stores a key-value pair, returning an error if it fails
func (kv *base[K, V]) TrySet(key K, value V) (V, error) {
	var keyParam, valueParam any
	var err error

	if kv.keyNeedsMarshal {
		keyParam, err = marshal(key)
		if err != nil {
			return value, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		keyParam = convertToNativeType(key)
	}

	if kv.valueNeedsMarshal {
		valueParam, err = marshal(value)
		if err != nil {
			return value, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		valueParam = convertToNativeType(value)
	}

	query := `
		WITH old AS (
			SELECT value FROM ` + kv.tableName + ` WHERE key = $1
		)
		INSERT INTO ` + kv.tableName + ` (key, value, expires_at)
		VALUES ($1, $2, NULL)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL
		RETURNING (SELECT value FROM old) AS old_value
	`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var oldValue V
	var oldValueScanned bool

	if kv.valueNeedsMarshal {
		var oldValueStr sql.NullString
		err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&oldValueStr)
		if err != nil {
			return value, err
		}
		if oldValueStr.Valid {
			oldValueTarget, getOldValue := newScanTarget[V]()
			unmarshal(oldValueStr.String, oldValueTarget)
			oldValue = getOldValue()
			oldValueScanned = true
		}
	} else {
		// For native types, use nullable scan target
		switch any(value).(type) {
		case string:
			var oldValueStr sql.NullString
			err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&oldValueStr)
			if err != nil {
				return value, err
			}
			if oldValueStr.Valid {
				oldValue = any(oldValueStr.String).(V)
				oldValueScanned = true
			}
		case []byte:
			var oldValueBytes []byte
			err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&oldValueBytes)
			if err != nil && err != pgx.ErrNoRows {
				return value, err
			}
			if err == nil && oldValueBytes != nil {
				oldValue = any(oldValueBytes).(V)
				oldValueScanned = true
			}
		case bool:
			var oldValueNullableBool sql.Null[bool]
			err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&oldValueNullableBool)
			if err != nil {
				return value, err
			}
			if oldValueNullableBool.Valid {
				oldValue = any(oldValueNullableBool.V).(V)
				oldValueScanned = true
			}
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			var oldValueNullable sql.Null[int64]
			err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&oldValueNullable)
			if err != nil {
				return value, err
			}
			if oldValueNullable.Valid {
				oldValueTarget, getOldValue := newScanTarget[V]()
				setInt(oldValueTarget, oldValueNullable.V)
				oldValue = getOldValue()
				oldValueScanned = true
			}
		case float32, float64:
			var oldValueNullableFloat sql.Null[float64]
			err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&oldValueNullableFloat)
			if err != nil {
				return value, err
			}
			if oldValueNullableFloat.Valid {
				oldValue = any(oldValueNullableFloat.V).(V)
				oldValueScanned = true
			}
		case time.Time, *time.Time:
			var oldValueNullable sql.Null[int64]
			err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&oldValueNullable)
			if err != nil {
				return value, err
			}
			if oldValueNullable.Valid && oldValueNullable.V != 0 {
				oldValue = any(time.Unix(0, oldValueNullable.V)).(V)
				oldValueScanned = true
			}
		}
	}

	if oldValueScanned {
		// Handle nil pointers before using cmp.Equal (which requires symmetric Equal methods)
		oldValueReflect := reflect.ValueOf(oldValue)
		valueReflect := reflect.ValueOf(value)
		oldIsNil := oldValueReflect.Kind() == reflect.Ptr && oldValueReflect.IsNil()
		valueIsNil := valueReflect.Kind() == reflect.Ptr && valueReflect.IsNil()

		// Only use cmp.Equal if both are non-nil
		changed := false
		if oldIsNil || valueIsNil {
			// If nil states differ, it's a change
			changed = oldIsNil != valueIsNil
		} else {
			// Both non-nil, safe to use cmp.Equal
			changed = !cmp.Equal(value, oldValue)
		}

		if changed {
			// slog.Default().Log(context.Background(), slog.LevelDebug, fmt.Sprintf("pgmap: %v: changed", kv.tableName), "key", key, "old", fmt.Sprintf("%v", oldValue), "new", fmt.Sprintf("%v", value))
		}
	}

	return value, nil
}

// Set stores a key-value pair, panics on error, returns the value.
// Clears any existing TTL (sets expires_at to NULL).
func (kv *base[K, V]) Set(key K, value V) V {
	_, err := kv.TrySet(key, value)
	if err != nil {
		panic(err)
	}
	return value
}

// TrySetWithTTL stores a key-value pair with a TTL, returning the value and an error if it fails
func (kv *base[K, V]) TrySetWithTTL(key K, value V, ttl time.Duration) (V, error) {
	var keyParam, valueParam any
	var err error

	if kv.keyNeedsMarshal {
		keyParam, err = marshal(key)
		if err != nil {
			return value, err
		}
	} else {
		keyParam = convertToNativeType(key)
	}

	if kv.valueNeedsMarshal {
		valueParam, err = marshal(value)
		if err != nil {
			return value, err
		}
	} else {
		valueParam = convertToNativeType(value)
	}

	expiresAt := time.Now().Add(ttl)

	query := `
		INSERT INTO ` + kv.tableName + ` (key, value, expires_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
	`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err = kv.db.Exec(ctx, query, keyParam, valueParam, expiresAt)
	return value, err
}

// SetWithTTL stores a key-value pair with a TTL, panics on error, returns the value
func (kv *base[K, V]) SetWithTTL(key K, value V, ttl time.Duration) V {
	_, err := kv.TrySetWithTTL(key, value, ttl)
	if err != nil {
		panic(err)
	}
	return value
}

// TryGet retrieves a value by key, returning an error if not found
func (kv *base[K, V]) TryGet(key K) (V, error) {
	var v V
	var keyParam any
	var err error

	if kv.keyNeedsMarshal {
		keyParam, err = marshal(key)
		if err != nil {
			return v, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		keyParam = convertToNativeType(key)
	}

	query := `SELECT value FROM ` + kv.tableName + ` WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW()) FOR UPDATE`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if kv.valueNeedsMarshal {
		var valueStr string
		err = kv.db.QueryRow(ctx, query, keyParam).Scan(&valueStr)
		if err != nil {
			return v, err
		}
		err = unmarshal(valueStr, &v)
		return v, err
	} else {
		// For native types, scan directly
		valueTarget, getValue := newScanTarget[V]()
		err = kv.db.QueryRow(ctx, query, keyParam).Scan(valueTarget)
		if err != nil {
			return v, err
		}
		return getValue(), nil
	}
}

// Get retrieves a value by key, panics on error
func (kv *base[K, V]) Get(key K) V {
	val, err := kv.TryGet(key)
	if err != nil {
		panic(err)
	}
	return val
}

// GetOr retrieves a value by key, returning defaultValue if not found
func (kv *base[K, V]) GetOr(key K, defaultValue V) V {
	val, err := kv.TryGet(key)
	if err == pgx.ErrNoRows {
		return defaultValue
	}
	if err != nil {
		panic(err)
	}
	return val
}

// TryHas checks if a key exists, returning an error if the check fails
func (kv *base[K, V]) TryHas(key K) (bool, error) {
	var keyParam any
	var err error

	if kv.keyNeedsMarshal {
		keyParam, err = marshal(key)
		if err != nil {
			return false, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		keyParam = convertToNativeType(key)
	}

	var exists int
	query := `SELECT 1 FROM ` + kv.tableName + ` WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW()) LIMIT 1`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = kv.db.QueryRow(ctx, query, keyParam).Scan(&exists)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Has checks if a key exists, panics on error
func (kv *base[K, V]) Has(key K) bool {
	exists, err := kv.TryHas(key)
	if err != nil {
		panic(err)
	}
	return exists
}

// TryDelete removes a key-value pair, returning an error if deletion fails
func (kv *base[K, V]) TryDelete(key K) error {
	var keyParam any
	var err error

	if kv.keyNeedsMarshal {
		keyParam, err = marshal(key)
		if err != nil {
			return err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		keyParam = convertToNativeType(key)
	}

	query := `DELETE FROM ` + kv.tableName + ` WHERE key = $1`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err = kv.db.Exec(ctx, query, keyParam)
	return err
}

// Delete removes a key-value pair, panics on error
func (kv *base[K, V]) Delete(key K) {
	if err := kv.TryDelete(key); err != nil {
		panic(err)
	}
}

// TryClear removes all key-value pairs, returning an error if it fails
func (kv *Map[K, V]) TryClear() error {
	query := `DELETE FROM ` + kv.tableName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := kv.db.Exec(ctx, query)
	return err
}

// Clear removes all key-value pairs, panics on error
func (kv *Map[K, V]) Clear() {
	if err := kv.TryClear(); err != nil {
		panic(err)
	}
}

// TryPurge removes the table, returning an error if it fails
func (kv *Map[K, V]) TryPurge() error {
	query := `DROP TABLE IF EXISTS ` + kv.tableName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := kv.db.Exec(ctx, query)
	return err
}

// Purge removes the table, panics on error
func (kv *Map[K, V]) Purge() {
	if err := kv.TryPurge(); err != nil {
		panic(err)
	}
}

// SetNX sets a value only if the key doesn't exist, returns true if the value was set
func (kv *Map[K, V]) SetNX(key K, value V) bool {
	wasSet, err := kv.TrySetNX(key, value)
	if err != nil {
		panic(err)
	}
	return wasSet
}

// TrySetNX sets a value only if the key doesn't exist, returns true if the value was set
func (kv *Map[K, V]) TrySetNX(key K, value V) (bool, error) {
	var keyParam, valueParam any
	var err error

	if kv.keyNeedsMarshal {
		keyParam, err = marshal(key)
		if err != nil {
			return false, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		keyParam = convertToNativeType(key)
	}

	if kv.valueNeedsMarshal {
		valueParam, err = marshal(value)
		if err != nil {
			return false, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		valueParam = convertToNativeType(value)
	}

	query := `
		INSERT INTO ` + kv.tableName + ` (key, value, expires_at)
		VALUES ($1, $2, NULL)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL
		WHERE ` + kv.tableName + `.expires_at IS NOT NULL AND ` + kv.tableName + `.expires_at <= NOW()
		RETURNING value
	`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if kv.valueNeedsMarshal {
		var resultStr string
		err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(&resultStr)
		if err == pgx.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	} else {
		// For native types, scan directly
		resultTarget, _ := newScanTarget[V]()
		err = kv.db.QueryRow(ctx, query, keyParam, valueParam).Scan(resultTarget)
		if err == pgx.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
}

// SetNZ sets a value only if it's not zero, returns the value
func (kv *Map[K, V]) SetNZ(key K, value V) V {
	var zero V
	// if value != zero {
	if !cmp.Equal(value, zero) {
		_, err := kv.TrySet(key, value)
		if err != nil {
			panic(err)
		}
		return value
	}
	// If value is zero, return existing value if it exists
	return kv.GetOr(key, value)
}

func (kv *Map[K, V]) AddRat(key K, delta any) *rat.Rational {
	ratOut, err := kv.TryAddRat(key, delta)
	if err != nil {
		panic(err)
	}
	return ratOut
}

func (kv *Map[K, V]) AddInt(key K, delta int) *rat.Rational {
	ratOut, err := kv.TryAddRat(key, delta)
	if err != nil {
		panic(err)
	}
	return ratOut
}

func (kv *Map[K, V]) TryAddRat(key K, delta any) (*rat.Rational, error) {
	var ratOut *rat.Rational
	out, err := kv.Update(key, func(v V) V {
		old := rat.Rat(v)
		ratOut = old.Add(delta)
		// slog.Default().Log(context.Background(), slog.LevelDebug, "pgmap: add", "key", key, "old", old, "new", ratOut, "delta", delta)
		return any(ratOut).(V)
	})
	if err != nil {
		return nil, err
	}
	return rat.Rat(out), nil
}

// Update atomically updates the value at key using the provided function and returns the new value
func (kv *Map[K, V]) Update(key K, fn func(V) V) (V, error) {
	var result V
	var keyParam any
	var err error

	if kv.keyNeedsMarshal {
		keyParam, err = marshal(key)
		if err != nil {
			return result, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		keyParam = convertToNativeType(key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	tx, err := kv.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)

	// Lock the row for update (skip expired entries)
	query := `SELECT value FROM ` + kv.tableName + ` WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW()) FOR UPDATE`

	var current V
	if kv.valueNeedsMarshal {
		var valueStr string
		err = tx.QueryRow(ctx, query, keyParam).Scan(&valueStr)
		if err == pgx.ErrNoRows {
			// Key doesn't exist or expired, use zero value
			current = result
		} else if err != nil {
			return result, err
		} else {
			err = unmarshal(valueStr, &current)
			if err != nil {
				return result, err
			}
		}
	} else {
		// For native types, scan directly
		valueTarget, getValue := newScanTarget[V]()
		err = tx.QueryRow(ctx, query, keyParam).Scan(valueTarget)
		if err == pgx.ErrNoRows {
			// Key doesn't exist or expired, use zero value
			current = result
		} else if err != nil {
			return result, err
		} else {
			current = getValue()
		}
	}

	// Apply function
	result = fn(current)

	// Update
	var resultParam any
	if kv.valueNeedsMarshal {
		resultParam, err = marshal(result)
		if err != nil {
			return result, err
		}
	} else {
		// For native types, convert to proper PostgreSQL types
		resultParam = convertToNativeType(result)
	}

	updateQuery := `
		INSERT INTO ` + kv.tableName + ` (key, value, expires_at)
		VALUES ($1, $2, NULL)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL
	`
	_, err = tx.Exec(ctx, updateQuery, keyParam, resultParam)
	if err != nil {
		return result, err
	}

	err = tx.Commit(ctx)
	return result, err
}

// convertToNativeType converts Go types to their PostgreSQL native representations
func convertToNativeType(v any) any {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return val
	case bool:
		return val
	case time.Time:
		if val.IsZero() {
			return int64(0)
		}
		return val.UnixNano()
	case *time.Time:
		if val == nil || val.IsZero() {
			return int64(0)
		}
		return val.UnixNano()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return toInt64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		return v
	}
}

func marshal(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case time.Time:
		if val.IsZero() {
			return "0", nil
		}
		return strconv.FormatInt(val.UnixNano(), 10), nil
	case *time.Time:
		if val == nil || val.IsZero() {
			return "0", nil
		}
		return strconv.FormatInt(val.UnixNano(), 10), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return strconv.FormatInt(toInt64(val), 10), nil
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	default:
		// Check if the type implements both sql.Scanner (for unmarshaling) and fmt.Stringer (for marshaling)
		// This allows types like *rat.Rational and *CustomScanner to use their String() method
		_, implementsScanner := v.(sql.Scanner)
		stringer, implementsStringer := v.(fmt.Stringer)

		// If pointer type and doesn't implement directly, check if dereferenced type implements
		if !implementsScanner || !implementsStringer {
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Ptr && !rv.IsNil() {
				deref := rv.Elem().Interface()
				if !implementsScanner {
					_, implementsScanner = deref.(sql.Scanner)
				}
				if !implementsStringer {
					stringer, implementsStringer = deref.(fmt.Stringer)
				}
			}
		}

		if implementsScanner && implementsStringer {
			return stringer.String(), nil
		}

		// For all other types, use JSON marshaling
		b, err := json.Marshal(v)
		return string(b), err
	}
}

func unmarshal(s string, v any) error {
	switch val := v.(type) {
	case *string:
		*val = s
		return nil
	case *time.Time:
		nanos, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		if nanos == 0 {
			*val = time.Time{}
		} else {
			*val = time.Unix(0, nanos)
		}
		return nil
	case **time.Time:
		nanos, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		if nanos == 0 {
			*val = nil
			return nil
		}
		t := time.Unix(0, nanos)
		*val = &t
		return nil
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		setInt(val, i)
		return nil
	case *float32:
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		*val = float32(f)
		return nil
	case *float64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		*val = f
		return nil
	default:
		// Check if v is **T where *T implements sql.Scanner
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() && rv.Elem().Kind() == reflect.Ptr {
			// v is **T, create new *T instance and scan into it
			elemType := rv.Elem().Type().Elem()
			newElem := reflect.New(elemType)
			if scanner, ok := newElem.Interface().(sql.Scanner); ok {
				if err := scanner.Scan(s); err != nil {
					return err
				}
				rv.Elem().Set(newElem)
				return nil
			}
		}

		// Check if v directly implements sql.Scanner
		if scanner, ok := v.(sql.Scanner); ok {
			return scanner.Scan(s)
		}

		return json.Unmarshal([]byte(s), v)
	}
}

func toInt64(v any) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	}
	return 0
}

func setInt(v any, i int64) {
	switch val := v.(type) {
	case *int:
		*val = int(i)
	case *int8:
		*val = int8(i)
	case *int16:
		*val = int16(i)
	case *int32:
		*val = int32(i)
	case *int64:
		*val = i
	case *uint:
		*val = uint(i)
	case *uint8:
		*val = uint8(i)
	case *uint16:
		*val = uint16(i)
	case *uint32:
		*val = uint32(i)
	case *uint64:
		*val = uint64(i)
	}
}

// newScanTarget creates a new scan target for type T.
// If T is a pointer type, it creates a new instance of the pointed-to type.
// For time.Time types, it returns an int64 target and converts on getValue.
// For float types, it returns a float64 target and converts on getValue.
func newScanTarget[T any]() (target any, getValue func() T) {
	var zero T
	rt := reflect.TypeOf(zero)

	// Handle string - scan directly as string
	if _, ok := any(zero).(string); ok {
		var s string
		return &s, func() T {
			return any(s).(T)
		}
	}

	// Handle []byte - scan directly as []byte
	if _, ok := any(zero).([]byte); ok {
		var b []byte
		return &b, func() T {
			return any(b).(T)
		}
	}

	// Handle bool - scan directly as bool
	if _, ok := any(zero).(bool); ok {
		var b bool
		return &b, func() T {
			return any(b).(T)
		}
	}

	// Handle time.Time - scan as int64 (UnixNano)
	if _, ok := any(zero).(time.Time); ok {
		var nanos int64
		return &nanos, func() T {
			if nanos == 0 {
				return any(time.Time{}).(T)
			}
			t := time.Unix(0, nanos)
			return any(t).(T)
		}
	}

	// Handle float types - scan directly as float64
	if _, ok := any(zero).(float32); ok {
		var f float64
		return &f, func() T {
			return any(float32(f)).(T)
		}
	}
	if _, ok := any(zero).(float64); ok {
		var f float64
		return &f, func() T {
			return any(f).(T)
		}
	}

	if rt != nil && rt.Kind() == reflect.Ptr {
		// Check if it's *time.Time
		if rt.Elem() == reflect.TypeOf(time.Time{}) {
			var nanos int64
			return &nanos, func() T {
				if nanos == 0 {
					return zero // nil pointer
				}
				t := time.Unix(0, nanos)
				return any(&t).(T)
			}
		}

		// Other pointer types
		elem := reflect.New(rt.Elem())
		return elem.Interface(), func() T {
			return elem.Interface().(T)
		}
	}

	// T is not a pointer, use standard approach
	target = new(T)
	return target, func() T {
		return *target.(*T)
	}
}

// Keys iterates over all keys in forward order
func (kv *base[K, V]) Keys(yield func(K) bool) {
	query := `SELECT key FROM ` + kv.tableName + ` WHERE (expires_at IS NULL OR expires_at > NOW()) ORDER BY key`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	rows, err := kv.db.Query(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (K, error) {
		if kv.keyNeedsMarshal {
			var keyStr string
			if err := row.Scan(&keyStr); err != nil {
				var zero K
				return zero, err
			}
			target, getValue := newScanTarget[K]()
			if err := unmarshal(keyStr, target); err != nil {
				var zero K
				return zero, err
			}
			return getValue(), nil
		}
		// Native type key (time.Time or integer) scanned directly from PostgreSQL
		target, getValue := newScanTarget[K]()
		err := row.Scan(target)
		return getValue(), err
	})
	if err != nil {
		panic(err)
	}

	for _, k := range keys {
		if !yield(k) {
			return
		}
	}
}

// KeysBackward iterates over all keys in reverse order
func (kv *base[K, V]) KeysBackward(yield func(K) bool) {
	query := `SELECT key FROM ` + kv.tableName + ` WHERE (expires_at IS NULL OR expires_at > NOW()) ORDER BY key DESC`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	rows, err := kv.db.Query(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (K, error) {
		if kv.keyNeedsMarshal {
			var keyStr string
			if err := row.Scan(&keyStr); err != nil {
				var zero K
				return zero, err
			}
			target, getValue := newScanTarget[K]()
			if err := unmarshal(keyStr, target); err != nil {
				var zero K
				return zero, err
			}
			return getValue(), nil
		}
		// Native type key (time.Time or integer) scanned directly from PostgreSQL
		target, getValue := newScanTarget[K]()
		err := row.Scan(target)
		return getValue(), err
	})
	if err != nil {
		panic(err)
	}

	for _, k := range keys {
		if !yield(k) {
			return
		}
	}
}

// All iterates over all key-value pairs in forward order
func (kv *base[K, V]) All(yield func(K, V) bool) {
	query := `SELECT key, value FROM ` + kv.tableName + ` WHERE (expires_at IS NULL OR expires_at > NOW()) ORDER BY key`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	rows, err := kv.db.Query(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	type kvPair struct {
		key   K
		value V
	}

	pairs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (kvPair, error) {
		var key K
		var value V

		keyTarget, getKey := newScanTarget[K]()
		valTarget, getVal := newScanTarget[V]()

		if kv.keyNeedsMarshal && kv.valueNeedsMarshal {
			// Both need unmarshaling
			var keyStr, valueStr string
			if err := row.Scan(&keyStr, &valueStr); err != nil {
				return kvPair{}, err
			}
			if err := unmarshal(keyStr, keyTarget); err != nil {
				return kvPair{}, err
			}
			if err := unmarshal(valueStr, valTarget); err != nil {
				return kvPair{}, err
			}
			key = getKey()
			value = getVal()
		} else if kv.keyNeedsMarshal && !kv.valueNeedsMarshal {
			// Key needs unmarshaling, value is native
			var keyStr string
			if err := row.Scan(&keyStr, valTarget); err != nil {
				return kvPair{}, err
			}
			if err := unmarshal(keyStr, keyTarget); err != nil {
				return kvPair{}, err
			}
			key = getKey()
			value = getVal()
		} else if !kv.keyNeedsMarshal && kv.valueNeedsMarshal {
			// Key is native, value needs unmarshaling
			var valueStr string
			if err := row.Scan(keyTarget, &valueStr); err != nil {
				return kvPair{}, err
			}
			if err := unmarshal(valueStr, valTarget); err != nil {
				return kvPair{}, err
			}
			key = getKey()
			value = getVal()
		} else {
			// Both are native
			if err := row.Scan(keyTarget, valTarget); err != nil {
				return kvPair{}, err
			}
			key = getKey()
			value = getVal()
		}

		return kvPair{key: key, value: value}, nil
	})
	if err != nil {
		panic(err)
	}

	for _, pair := range pairs {
		if !yield(pair.key, pair.value) {
			return
		}
	}
}

// KeysWhere returns an iterator over keys matching the WHERE clause in forward order
func (kv *base[K, V]) KeysWhere(whereClause string) func(yield func(K) bool) {
	return func(yield func(K) bool) {
		query := `SELECT key FROM ` + kv.tableName + ` WHERE (expires_at IS NULL OR expires_at > NOW()) AND (` + whereClause + `) ORDER BY key`
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		rows, err := kv.db.Query(ctx, query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (K, error) {
			if kv.keyNeedsMarshal {
				var keyStr string
				if err := row.Scan(&keyStr); err != nil {
					var zero K
					return zero, err
				}
				target, getValue := newScanTarget[K]()
				if err := unmarshal(keyStr, target); err != nil {
					var zero K
					return zero, err
				}
				return getValue(), nil
			}
			// Native type key (time.Time or integer) scanned directly from PostgreSQL
			target, getValue := newScanTarget[K]()
			err := row.Scan(target)
			return getValue(), err
		})
		if err != nil {
			panic(err)
		}

		for _, k := range keys {
			if !yield(k) {
				return
			}
		}
	}
}

// LockMode specifies the PostgreSQL table lock mode
type LockMode int

const (
	// LockShare allows concurrent reads, blocks writes
	LockShare LockMode = iota
	// LockExclusive blocks writes, allows reads (but not concurrent SHARE locks)
	LockExclusive
	// LockAccessExclusive blocks everything including reads
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
		return "EXCLUSIVE"
	}
}

// Tx represents a transaction-bound Map with Set/Get/Delete operations
type Tx[K comparable, V any] struct {
	base[K, V]
}

// TryWithLock acquires a table-level lock and executes the function within a transaction.
// The lock is released when the transaction commits or rolls back.
// Returns error from the function or transaction operations.
func (kv *Map[K, V]) TryWithLock(mode LockMode, fn func(tx *Tx[K, V]) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	tx, err := kv.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Lock the table
	lockQuery := `LOCK TABLE ` + kv.tableName + ` IN ` + mode.String() + ` MODE`
	_, err = tx.Exec(ctx, lockQuery)
	if err != nil {
		return err
	}

	// Create transaction-bound Map
	txMap := &Tx[K, V]{
		base: base[K, V]{
			db:                tx,
			tableName:         kv.tableName,
			keyNeedsMarshal:   kv.keyNeedsMarshal,
			valueNeedsMarshal: kv.valueNeedsMarshal,
		},
	}

	// Execute user function
	if err := fn(txMap); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// WithLock acquires a table-level lock and executes the function within a transaction, panics on error
func (kv *Map[K, V]) WithLock(mode LockMode, fn func(tx *Tx[K, V]) error) {
	if err := kv.TryWithLock(mode, fn); err != nil {
		panic(err)
	}
}

// AllWhere returns an iterator over key-value pairs matching the WHERE clause in forward order
func (kv *base[K, V]) AllWhere(whereClause string) func(yield func(K, V) bool) {
	return func(yield func(K, V) bool) {
		query := `SELECT key, value FROM ` + kv.tableName + ` WHERE (expires_at IS NULL OR expires_at > NOW()) AND (` + whereClause + `) ORDER BY key`
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		rows, err := kv.db.Query(ctx, query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		type kvPair struct {
			key   K
			value V
		}

		pairs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (kvPair, error) {
			var key K
			var value V

			keyTarget, getKey := newScanTarget[K]()
			valTarget, getVal := newScanTarget[V]()

			if kv.keyNeedsMarshal && kv.valueNeedsMarshal {
				// Both need unmarshaling
				var keyStr, valueStr string
				if err := row.Scan(&keyStr, &valueStr); err != nil {
					return kvPair{}, err
				}
				if err := unmarshal(keyStr, keyTarget); err != nil {
					return kvPair{}, err
				}
				if err := unmarshal(valueStr, valTarget); err != nil {
					return kvPair{}, err
				}
				key = getKey()
				value = getVal()
			} else if kv.keyNeedsMarshal && !kv.valueNeedsMarshal {
				// Key needs unmarshaling, value is native
				var keyStr string
				if err := row.Scan(&keyStr, valTarget); err != nil {
					return kvPair{}, err
				}
				if err := unmarshal(keyStr, keyTarget); err != nil {
					return kvPair{}, err
				}
				key = getKey()
				value = getVal()
			} else if !kv.keyNeedsMarshal && kv.valueNeedsMarshal {
				// Key is native, value needs unmarshaling
				var valueStr string
				if err := row.Scan(keyTarget, &valueStr); err != nil {
					return kvPair{}, err
				}
				if err := unmarshal(valueStr, valTarget); err != nil {
					return kvPair{}, err
				}
				key = getKey()
				value = getVal()
			} else {
				// Both are native
				if err := row.Scan(keyTarget, valTarget); err != nil {
					return kvPair{}, err
				}
				key = getKey()
				value = getVal()
			}

			return kvPair{key: key, value: value}, nil
		})
		if err != nil {
			panic(err)
		}

		for _, pair := range pairs {
			if !yield(pair.key, pair.value) {
				return
			}
		}
	}
}
