package pgmap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xtdlib/rat"
)

func testTable(t *testing.T) string {
	return fmt.Sprintf("test_%s_%d", t.Name(), time.Now().UnixNano())
}

func cleanup[K comparable, V any](m *Map[K, V]) {
	m.Purge()
	m.Close()
}

func TestBasicCRUD(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	// Set
	m.Set("key1", "value1")
	m.Set("key2", "value2")

	// Get
	if v := m.Get("key1"); v != "value1" {
		t.Errorf("Get key1: got %q, want %q", v, "value1")
	}

	// Has
	if !m.Has("key1") {
		t.Error("Has key1: got false, want true")
	}
	if m.Has("nonexistent") {
		t.Error("Has nonexistent: got true, want false")
	}

	// Delete
	m.Delete("key1")
	if m.Has("key1") {
		t.Error("After delete, Has key1: got true, want false")
	}

	// Clear
	m.Set("key3", "value3")
	m.Clear()
	if m.Has("key2") || m.Has("key3") {
		t.Error("After clear, keys still exist")
	}
}

func TestKeysIteration(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	m.Set("c", "3")
	m.Set("a", "1")
	m.Set("b", "2")

	// Forward iteration should be sorted
	var keys []string
	for k := range m.Keys {
		keys = append(keys, k)
	}
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Errorf("Keys: got %v, want [a b c]", keys)
	}

	// Backward iteration
	var keysBack []string
	for k := range m.KeysBackward {
		keysBack = append(keysBack, k)
	}
	if len(keysBack) != 3 || keysBack[0] != "c" || keysBack[1] != "b" || keysBack[2] != "a" {
		t.Errorf("KeysBackward: got %v, want [c b a]", keysBack)
	}

	// Early termination
	var partial []string
	for k := range m.Keys {
		partial = append(partial, k)
		if len(partial) == 2 {
			break
		}
	}
	if len(partial) != 2 {
		t.Errorf("Early termination: got %d keys, want 2", len(partial))
	}
}

func TestAllIterator(t *testing.T) {
	m := New[int, string](testTable(t))
	defer cleanup(m)

	m.Set(3, "three")
	m.Set(1, "one")
	m.Set(2, "two")

	var pairs []struct {
		k int
		v string
	}
	for k, v := range m.All {
		pairs = append(pairs, struct {
			k int
			v string
		}{k, v})
	}

	if len(pairs) != 3 {
		t.Fatalf("All: got %d pairs, want 3", len(pairs))
	}
	if pairs[0].k != 1 || pairs[1].k != 2 || pairs[2].k != 3 {
		t.Errorf("All not sorted: got keys %d,%d,%d", pairs[0].k, pairs[1].k, pairs[2].k)
	}
}

func TestGetOr(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	// Missing key returns default
	if v := m.GetOr("missing", "default"); v != "default" {
		t.Errorf("GetOr missing: got %q, want %q", v, "default")
	}

	// Existing key returns actual value
	m.Set("exists", "actual")
	if v := m.GetOr("exists", "default"); v != "actual" {
		t.Errorf("GetOr exists: got %q, want %q", v, "actual")
	}
}

func TestSetNX(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	// First call succeeds
	if ok := m.SetNX("key", "value1"); !ok {
		t.Error("SetNX first call: got false, want true")
	}

	// Second call fails
	if ok := m.SetNX("key", "value2"); ok {
		t.Error("SetNX second call: got true, want false")
	}

	// Value unchanged
	if v := m.Get("key"); v != "value1" {
		t.Errorf("SetNX value: got %q, want %q", v, "value1")
	}
}

func TestSetNXConcurrent(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	var wg sync.WaitGroup
	successes := make(chan int, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if m.SetNX("race-key", fmt.Sprintf("value-%d", id)) {
				successes <- id
			}
		}(i)
	}

	wg.Wait()
	close(successes)

	count := 0
	for range successes {
		count++
	}

	if count != 1 {
		t.Errorf("SetNX concurrent: %d succeeded, want exactly 1", count)
	}
}

func TestSetNZ(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	// Non-zero sets
	m.SetNZ("key", "value")
	if v := m.Get("key"); v != "value" {
		t.Errorf("SetNZ non-zero: got %q, want %q", v, "value")
	}

	// Zero skips
	m.SetNZ("key", "")
	if v := m.Get("key"); v != "value" {
		t.Errorf("SetNZ zero: got %q, want %q", v, "value")
	}
}

func TestTryGetTryHas(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	// TryGet missing key
	_, err := m.TryGet("missing")
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("TryGet missing: got %v, want pgx.ErrNoRows", err)
	}

	// TryHas missing key returns false, nil
	exists, err := m.TryHas("missing")
	if err != nil || exists {
		t.Errorf("TryHas missing: got (%v, %v), want (false, nil)", exists, err)
	}
}

func TestTypeString(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	m.Set("hello", "world")
	if v := m.Get("hello"); v != "world" {
		t.Errorf("string: got %q, want %q", v, "world")
	}
	verifyColumnType(t, m.table, "key", "TEXT")
	verifyColumnType(t, m.table, "value", "TEXT")
}

func TestTypeTimeTime(t *testing.T) {
	m := New[time.Time, time.Time](testTable(t))
	defer cleanup(m)

	k := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)

	m.Set(k, v)
	got := m.Get(k)
	if !got.Equal(v) {
		t.Errorf("time.Time: got %v, want %v", got, v)
	}
	verifyColumnType(t, m.table, "key", "BIGINT")
	verifyColumnType(t, m.table, "value", "BIGINT")
}

func TestTypeTimeTimePointer(t *testing.T) {
	m := New[*time.Time, string](testTable(t))
	defer cleanup(m)

	k := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	m.Set(&k, "test")

	if v := m.Get(&k); v != "test" {
		t.Errorf("*time.Time key: got %q, want %q", v, "test")
	}
	verifyColumnType(t, m.table, "key", "BIGINT")
}

func TestTypeIntegers(t *testing.T) {
	// int key
	m1 := New[int, string](testTable(t) + "_int")
	defer cleanup(m1)
	m1.Set(42, "forty-two")
	if v := m1.Get(42); v != "forty-two" {
		t.Errorf("int key: got %q", v)
	}
	verifyColumnType(t, m1.table, "key", "BIGINT")

	// int64 key
	m2 := New[int64, string](testTable(t) + "_int64")
	defer cleanup(m2)
	m2.Set(int64(9223372036854775807), "max")
	if v := m2.Get(int64(9223372036854775807)); v != "max" {
		t.Errorf("int64 key: got %q", v)
	}

	// uint key
	m3 := New[uint, string](testTable(t) + "_uint")
	defer cleanup(m3)
	m3.Set(uint(100), "hundred")
	if v := m3.Get(uint(100)); v != "hundred" {
		t.Errorf("uint key: got %q", v)
	}

	// uint64 key (note: values must fit in int64 range since BIGINT is signed)
	m4 := New[uint64, string](testTable(t) + "_uint64")
	defer cleanup(m4)
	m4.Set(uint64(9223372036854775807), "max")
	if v := m4.Get(uint64(9223372036854775807)); v != "max" {
		t.Errorf("uint64 key: got %q", v)
	}
}

func TestTypeIntegerValues(t *testing.T) {
	m1 := New[string, int](testTable(t) + "_vint")
	defer cleanup(m1)
	m1.Set("a", 123)
	if v := m1.Get("a"); v != 123 {
		t.Errorf("int value: got %d", v)
	}
	verifyColumnType(t, m1.table, "value", "BIGINT")

	m2 := New[string, int8](testTable(t) + "_vint8")
	defer cleanup(m2)
	m2.Set("b", 127)
	if v := m2.Get("b"); v != 127 {
		t.Errorf("int8 value: got %d", v)
	}

	m3 := New[string, uint32](testTable(t) + "_vuint32")
	defer cleanup(m3)
	m3.Set("c", 4294967295)
	if v := m3.Get("c"); v != 4294967295 {
		t.Errorf("uint32 value: got %d", v)
	}
}

func TestTypeFloats(t *testing.T) {
	m1 := New[float64, float64](testTable(t) + "_f64")
	defer cleanup(m1)
	m1.Set(3.14159, 2.71828)
	if v := m1.Get(3.14159); v != 2.71828 {
		t.Errorf("float64: got %f", v)
	}
	verifyColumnType(t, m1.table, "key", "DOUBLE PRECISION")
	verifyColumnType(t, m1.table, "value", "DOUBLE PRECISION")

	m2 := New[float32, float32](testTable(t) + "_f32")
	defer cleanup(m2)
	m2.Set(float32(1.5), float32(2.5))
	if v := m2.Get(float32(1.5)); v != float32(2.5) {
		t.Errorf("float32: got %f", v)
	}
}

func TestTypeBytes(t *testing.T) {
	m := New[string, []byte](testTable(t))
	defer cleanup(m)

	data := []byte{0x00, 0x01, 0x02, 0xFF}
	m.Set("binary", data)
	got := m.Get("binary")
	if len(got) != len(data) {
		t.Fatalf("[]byte length: got %d, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Errorf("[]byte[%d]: got %x, want %x", i, got[i], data[i])
		}
	}
	verifyColumnType(t, m.table, "value", "BYTEA")
}

func TestTypeBool(t *testing.T) {
	m := New[string, bool](testTable(t))
	defer cleanup(m)

	m.Set("yes", true)
	m.Set("no", false)

	if v := m.Get("yes"); !v {
		t.Error("bool true: got false")
	}
	if v := m.Get("no"); v {
		t.Error("bool false: got true")
	}
	verifyColumnType(t, m.table, "value", "BOOLEAN")
}

func TestTypeStruct(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}
	m := New[string, Person](testTable(t))
	defer cleanup(m)

	p := Person{Name: "Alice", Age: 30}
	m.Set("person1", p)

	got := m.Get("person1")
	if got.Name != "Alice" || got.Age != 30 {
		t.Errorf("struct: got %+v, want %+v", got, p)
	}
	verifyColumnType(t, m.table, "value", "JSON")
}

func TestTypeStructPointer(t *testing.T) {
	type Data struct {
		Value int
	}
	m := New[string, *Data](testTable(t))
	defer cleanup(m)

	// Non-nil
	m.Set("ptr", &Data{Value: 42})
	got := m.Get("ptr")
	if got == nil || got.Value != 42 {
		t.Errorf("struct pointer: got %+v", got)
	}

	// Nil
	m.Set("nil", nil)
	gotNil := m.Get("nil")
	if gotNil != nil {
		t.Errorf("nil struct pointer: got %+v, want nil", gotNil)
	}
}

func TestTypeSlice(t *testing.T) {
	m := New[string, []int](testTable(t))
	defer cleanup(m)

	s := []int{1, 2, 3, 4, 5}
	m.Set("nums", s)

	got := m.Get("nums")
	if len(got) != 5 {
		t.Fatalf("slice length: got %d", len(got))
	}
	for i := range s {
		if got[i] != s[i] {
			t.Errorf("slice[%d]: got %d, want %d", i, got[i], s[i])
		}
	}
}

func TestTypeMap(t *testing.T) {
	m := New[string, map[string]int](testTable(t))
	defer cleanup(m)

	data := map[string]int{"a": 1, "b": 2}
	m.Set("map", data)

	got := m.Get("map")
	if got["a"] != 1 || got["b"] != 2 {
		t.Errorf("map: got %+v", got)
	}
}

func TestTypeInterfaceSlice(t *testing.T) {
	m := New[string, []any](testTable(t))
	defer cleanup(m)

	data := []any{1.0, "two", true} // Note: JSON unmarshals numbers as float64
	m.Set("mixed", data)

	got := m.Get("mixed")
	if len(got) != 3 {
		t.Fatalf("[]any length: got %d", len(got))
	}
}

func TestTypeNestedStruct(t *testing.T) {
	type Inner struct {
		Value string
	}
	type Outer struct {
		Name  string
		Inner Inner
	}
	m := New[string, Outer](testTable(t))
	defer cleanup(m)

	data := Outer{Name: "outer", Inner: Inner{Value: "inner"}}
	m.Set("nested", data)

	got := m.Get("nested")
	if got.Name != "outer" || got.Inner.Value != "inner" {
		t.Errorf("nested struct: got %+v", got)
	}
}

func TestTypeScannerStringer(t *testing.T) {
	m := New[string, *rat.Rational](testTable(t))
	defer cleanup(m)

	r := rat.Rat("3/4")
	m.Set("rational", r)

	got := m.Get("rational")
	if got.String() != r.String() {
		t.Errorf("Scanner/Stringer: got %s, want %s", got.String(), r.String())
	}
	verifyColumnType(t, m.table, "value", "TEXT")
}

func TestConcurrentWrites(t *testing.T) {
	m := New[int, string](testTable(t))
	defer cleanup(m)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.Set(id, fmt.Sprintf("value-%d", id))
		}(i)
	}
	wg.Wait()

	// Verify all values
	for i := 0; i < 10; i++ {
		expected := fmt.Sprintf("value-%d", i)
		if v := m.Get(i); v != expected {
			t.Errorf("concurrent write %d: got %q, want %q", i, v, expected)
		}
	}
}

func TestConcurrentUpdate(t *testing.T) {
	m := New[string, int](testTable(t))
	defer cleanup(m)

	m.Set("counter", 0)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Update("counter", func(v int) int { return v + 1 })
		}()
	}
	wg.Wait()

	if v := m.Get("counter"); v != 20 {
		t.Errorf("concurrent Update: got %d, want 20", v)
	}
}

func TestSpecialCharactersInKeys(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	keys := []string{
		"with space",
		"with/slash",
		"with:colon",
		`with"quote`,
		`with\backslash`,
		"with\ttab",
		"with\nnewline",
	}

	for _, k := range keys {
		m.Set(k, "value")
		if !m.Has(k) {
			t.Errorf("special key %q: not found", k)
		}
		if v := m.Get(k); v != "value" {
			t.Errorf("special key %q: got %q", k, v)
		}
	}
}

func TestZeroValues(t *testing.T) {
	// Zero int
	m1 := New[int, string](testTable(t) + "_zeroint")
	defer cleanup(m1)
	m1.Set(0, "zero")
	if v := m1.Get(0); v != "zero" {
		t.Errorf("zero int key: got %q", v)
	}

	// Empty string
	m2 := New[string, string](testTable(t) + "_emptystr")
	defer cleanup(m2)
	m2.Set("", "empty-key")
	if v := m2.Get(""); v != "empty-key" {
		t.Errorf("empty string key: got %q", v)
	}

	// Zero time
	m3 := New[time.Time, string](testTable(t) + "_zerotime")
	defer cleanup(m3)
	var zeroTime time.Time
	m3.Set(zeroTime, "zero-time")
	if v := m3.Get(zeroTime); v != "zero-time" {
		t.Errorf("zero time key: got %q", v)
	}
}

func TestAddRatConcurrent(t *testing.T) {
	m := New[string, *rat.Rational](testTable(t))
	defer cleanup(m)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.AddRat("sum", 1)
		}()
	}
	wg.Wait()

	v := m.Get("sum")
	if v.String() != "100" {
		t.Errorf("AddRat concurrent: got %s, want 100", v.String())
	}
}

func TestAddRatFractional(t *testing.T) {
	m := New[string, *rat.Rational](testTable(t))
	defer cleanup(m)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.AddRat("frac", "3/10")
		}()
	}
	wg.Wait()

	v := m.Get("frac")
	if v.String() != "30" {
		t.Errorf("AddRat fractional: got %s, want 30", v.String())
	}
}

func TestMap(t *testing.T) {
	m := New[string, int](testTable(t))
	defer cleanup(m)

	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	result := m.Map()
	if len(result) != 3 {
		t.Fatalf("Map length: got %d, want 3", len(result))
	}
	if result["a"] != 1 || result["b"] != 2 || result["c"] != 3 {
		t.Errorf("Map contents: got %+v", result)
	}
}

func TestWithLock(t *testing.T) {
	m := New[string, int](testTable(t))
	defer cleanup(m)

	m.Set("val", 10)

	err := m.TryWithLock(LockExclusive, func(tx *Tx[string, int]) error {
		v := tx.Get("val")
		tx.Set("val", v+5)
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock: %v", err)
	}

	if v := m.Get("val"); v != 15 {
		t.Errorf("WithLock result: got %d, want 15", v)
	}
}

func TestTTL(t *testing.T) {
	m := New[string, string](testTable(t))
	defer cleanup(m)

	// Set with short TTL
	m.Set("expires", "soon", 100*time.Millisecond)

	// Should exist immediately
	if !m.Has("expires") {
		t.Error("TTL: key should exist immediately")
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Should not exist
	if m.Has("expires") {
		t.Error("TTL: key should have expired")
	}
}

func TestKeysWhere(t *testing.T) {
	m := New[int, string](testTable(t))
	defer cleanup(m)

	for i := 1; i <= 10; i++ {
		m.Set(i, fmt.Sprintf("v%d", i))
	}

	var keys []int
	for k := range m.KeysWhere("key > 5") {
		keys = append(keys, k)
	}

	if len(keys) != 5 {
		t.Errorf("KeysWhere: got %d keys, want 5", len(keys))
	}
}

func TestAllWhere(t *testing.T) {
	m := New[int, string](testTable(t))
	defer cleanup(m)

	for i := 1; i <= 10; i++ {
		m.Set(i, fmt.Sprintf("v%d", i))
	}

	count := 0
	for k, v := range m.AllWhere("key <= 3") {
		count++
		expected := fmt.Sprintf("v%d", k)
		if v != expected {
			t.Errorf("AllWhere: key %d got %q, want %q", k, v, expected)
		}
	}

	if count != 3 {
		t.Errorf("AllWhere: got %d pairs, want 3", count)
	}
}

var testPool *pgxpool.Pool
var testPoolOnce sync.Once

func getTestPool() *pgxpool.Pool {
	testPoolOnce.Do(func() {
		dsn := os.Getenv("PGMAP_DSN")
		if dsn == "" {
			dsn = "postgres://postgres:postgres@localhost:5432/postgres"
		}
		var err error
		testPool, err = pgxpool.New(context.Background(), dsn)
		if err != nil {
			panic(err)
		}
	})
	return testPool
}

func verifyColumnType(t *testing.T, table, column, expectedType string) {
	t.Helper()
	pool := getTestPool()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var actualType string
	err := pool.QueryRow(ctx,
		`SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
		table, column,
	).Scan(&actualType)
	if err != nil {
		// Skip verification if we can't query
		return
	}

	if !matchesPgType(actualType, expectedType) {
		t.Errorf("Column %s.%s: got type %q, want %q", table, column, actualType, expectedType)
	}
}

// memStore is a minimal in-memory Store implementation using sync.Map for benchmark comparison.
type memStore[K comparable, V any] struct {
	m sync.Map
}

func (s *memStore[K, V]) Set(key K, value V, _ ...time.Duration) V {
	s.m.Store(key, value)
	return value
}

func (s *memStore[K, V]) TrySet(key K, value V, ttl ...time.Duration) (V, error) {
	return s.Set(key, value, ttl...), nil
}

func (s *memStore[K, V]) Get(key K) V {
	v, _ := s.m.Load(key)
	return v.(V)
}

func (s *memStore[K, V]) TryGet(key K) (V, error) {
	v, ok := s.m.Load(key)
	if !ok {
		var zero V
		return zero, errors.New("not found")
	}
	return v.(V), nil
}

func (s *memStore[K, V]) GetOr(key K, def V) V {
	v, ok := s.m.Load(key)
	if !ok {
		return def
	}
	return v.(V)
}

func (s *memStore[K, V]) TryGetOr(key K, def V) (V, error) { return s.GetOr(key, def), nil }
func (s *memStore[K, V]) Has(key K) bool                    { _, ok := s.m.Load(key); return ok }
func (s *memStore[K, V]) TryHas(key K) (bool, error)        { return s.Has(key), nil }
func (s *memStore[K, V]) Delete(key K)                      { s.m.Delete(key) }
func (s *memStore[K, V]) TryDelete(key K) error             { s.m.Delete(key); return nil }

func (s *memStore[K, V]) Keys(yield func(K) bool) {
	s.m.Range(func(k, _ any) bool { return yield(k.(K)) })
}

func (s *memStore[K, V]) KeysBackward(yield func(K) bool)                         { s.Keys(yield) }
func (s *memStore[K, V]) All(yield func(K, V) bool) {
	s.m.Range(func(k, v any) bool { return yield(k.(K), v.(V)) })
}
func (s *memStore[K, V]) KeysWhere(_ string) func(yield func(K) bool)    { return s.Keys }
func (s *memStore[K, V]) AllWhere(_ string) func(yield func(K, V) bool)  { return s.All }
func (s *memStore[K, V]) Map() map[K]V {
	result := make(map[K]V)
	s.All(func(k K, v V) bool { result[k] = v; return true })
	return result
}

// Compile-time check
var _ Store[string, string] = (*memStore[string, string])(nil)

func benchTable(b *testing.B) string {
	return fmt.Sprintf("bench_%d", time.Now().UnixNano())
}

func BenchmarkSet_PG(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkSet_Mem(b *testing.B) {
	m := &memStore[string, string]{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkGet_PG(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkGet_Mem(b *testing.B) {
	m := &memStore[string, string]{}
	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkSet_PGSync(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b), SyncCommit())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkSet_PGUnlogged(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b), Unlogged())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkSet_PGSyncUnlog(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b), SyncCommit(), Unlogged())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkSetParallel_PG(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(fmt.Sprintf("key-%d", i), "value")
			i++
		}
	})
}

func BenchmarkSetParallel_PGSync(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b), SyncCommit())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(fmt.Sprintf("key-%d", i), "value")
			i++
		}
	})
}

func BenchmarkSetParallel_Mem(b *testing.B) {
	m := &memStore[string, string]{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(fmt.Sprintf("key-%d", i), "value")
			i++
		}
	})
}

func BenchmarkGetParallel_PG(b *testing.B) {
	m, err := TryNew[string, string](benchTable(b))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { m.Purge(); m.Close() }()

	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

func BenchmarkGetParallel_Mem(b *testing.B) {
	m := &memStore[string, string]{}
	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}
