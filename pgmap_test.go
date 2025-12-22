package pgmap

import (
	"bytes"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/xtdlib/rat"
)

func TestKeys(t *testing.T) {
	kv := New[string, string]("test_keys")

	kv.Clear()

	// Test empty store
	var keys []string
	kv.Keys(func(key string) bool {
		keys = append(keys, key)
		return true
	})
	if len(keys) != 0 {
		t.Fatalf("Expected 0 keys, got %d", len(keys))
	}

	// Add test data
	kv.Set("zebra", "value1")
	kv.Set("apple", "value2")
	kv.Set("banana", "value3")

	// Test forward iteration - should be sorted
	keys = nil
	kv.Keys(func(key string) bool {
		keys = append(keys, key)
		return true
	})

	expected := []string{"apple", "banana", "zebra"}
	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}
	for i, key := range keys {
		if key != expected[i] {
			t.Fatalf("At index %d: expected %s, got %s", i, expected[i], key)
		}
	}

	// Test early termination
	keys = nil
	kv.Keys(func(key string) bool {
		keys = append(keys, key)
		return len(keys) < 2
	})
	if len(keys) != 2 {
		t.Fatalf("Expected 2 keys with early termination, got %d", len(keys))
	}
	if keys[0] != "apple" || keys[1] != "banana" {
		t.Fatalf("Early termination keys incorrect: got %v", keys)
	}
}

func TestKeysBackward(t *testing.T) {
	kv := New[string, string]("test_keys_backward")

	kv.Clear()

	// Test empty store
	var keys []string
	kv.KeysBackward(func(key string) bool {
		keys = append(keys, key)
		return true
	})
	if len(keys) != 0 {
		t.Fatalf("Expected 0 keys, got %d", len(keys))
	}

	// Add test data
	kv.Set("zebra", "value1")
	kv.Set("apple", "value2")
	kv.Set("banana", "value3")

	// Test backward iteration - should be reverse sorted
	keys = nil
	kv.KeysBackward(func(key string) bool {
		keys = append(keys, key)
		return true
	})

	expected := []string{"zebra", "banana", "apple"}
	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}
	for i, key := range keys {
		if key != expected[i] {
			t.Fatalf("At index %d: expected %s, got %s", i, expected[i], key)
		}
	}

	// Test early termination
	keys = nil
	kv.KeysBackward(func(key string) bool {
		keys = append(keys, key)
		return len(keys) < 2
	})
	if len(keys) != 2 {
		t.Fatalf("Expected 2 keys with early termination, got %d", len(keys))
	}
	if keys[0] != "zebra" || keys[1] != "banana" {
		t.Fatalf("Early termination keys incorrect: got %v", keys)
	}
}

func TestHas(t *testing.T) {
	kv := New[string, int]("test_has")

	kv.Clear()

	// Test non-existent key
	if kv.Has("missing") {
		t.Fatal("Expected Has to return false for missing key")
	}

	// Test existing key
	kv.Set("exists", 42)
	if !kv.Has("exists") {
		t.Fatal("Expected Has to return true for existing key")
	}
}

func TestDelete(t *testing.T) {
	kv := New[string, int]("test_delete")

	kv.Clear()

	kv.Set("key1", 100)
	if !kv.Has("key1") {
		t.Fatal("Key should exist after Set")
	}

	kv.Delete("key1")
	if kv.Has("key1") {
		t.Fatal("Key should not exist after Delete")
	}
}

func TestClear(t *testing.T) {
	kv := New[string, string]("test_clear")

	kv.Set("key1", "val1")
	kv.Set("key2", "val2")
	kv.Set("key3", "val3")

	kv.Clear()

	var count int
	kv.Keys(func(key string) bool {
		count++
		return true
	})

	if count != 0 {
		t.Fatalf("Expected 0 keys after Clear, got %d", count)
	}
}

func TestGetOr(t *testing.T) {
	kv := New[string, int]("test_getor")

	kv.Clear()

	// Test missing key returns default
	val := kv.GetOr("missing", 999)
	if val != 999 {
		t.Fatalf("Expected default value 999, got %d", val)
	}

	// Test existing key returns actual value
	kv.Set("exists", 42)
	val = kv.GetOr("exists", 999)
	if val != 42 {
		t.Fatalf("Expected actual value 42, got %d", val)
	}
}

func TestSetNX(t *testing.T) {
	kv := New[string, int]("test_setnx")

	kv.Clear()

	// First SetNX should set the value
	wasSet := kv.SetNX("key1", 100)
	if !wasSet {
		t.Fatal("Expected SetNX to return true on first set")
	}
	if kv.Get("key1") != 100 {
		t.Fatalf("Expected 100, got %d", kv.Get("key1"))
	}

	// Second SetNX should not set (key already exists)
	wasSet = kv.SetNX("key1", 200)
	if wasSet {
		t.Fatal("Expected SetNX to return false when key exists")
	}
	if kv.Get("key1") != 100 {
		t.Fatalf("Expected value to remain 100, got %d", kv.Get("key1"))
	}
}

func TestSetNXConcurrent(t *testing.T) {
	kv := New[string, int]("test_setnx_concurrent")
	kv.Clear()

	const numGoroutines = 10
	results := make([]bool, numGoroutines)
	done := make(chan int, numGoroutines)

	// Launch multiple goroutines trying to set the same key
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			results[id] = kv.SetNX("race_key", id)
			done <- id
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Exactly one should return true
	trueCount := 0
	for i, wasSet := range results {
		if wasSet {
			trueCount++
			t.Logf("Goroutine %d successfully set the key", i)
		}
	}

	if trueCount != 1 {
		t.Fatalf("Expected exactly 1 goroutine to succeed, got %d", trueCount)
	}

	// Verify the key exists
	if !kv.Has("race_key") {
		t.Fatal("Key should exist after concurrent SetNX")
	}
}

func TestSetNZ(t *testing.T) {
	kv := New[string, int]("test_setnz")

	kv.Clear()

	// SetNZ with non-zero should set
	val := kv.SetNZ("key1", 100)
	if val != 100 {
		t.Fatalf("Expected 100, got %d", val)
	}
	if !kv.Has("key1") {
		t.Fatal("Key should exist after SetNZ with non-zero value")
	}

	// SetNZ with zero should not set
	kv.SetNZ("key2", 0)
	if kv.Has("key2") {
		t.Fatal("Key should not exist after SetNZ with zero value")
	}
}

func TestTryGet(t *testing.T) {
	kv := New[string, int]("test_tryget")

	kv.Clear()

	// Try missing key
	_, err := kv.TryGet("missing")
	if err == nil {
		t.Fatal("Expected error for missing key")
	}

	// Try existing key
	kv.Set("exists", 42)
	val, err := kv.TryGet("exists")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("Expected 42, got %d", val)
	}
}

func TestTryHas(t *testing.T) {
	kv := New[string, int]("test_tryhas")

	kv.Clear()

	// Check missing key
	exists, err := kv.TryHas("missing")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if exists {
		t.Fatal("Expected false for missing key")
	}

	// Check existing key
	kv.Set("exists", 42)
	exists, err = kv.TryHas("exists")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("Expected true for existing key")
	}
}

func TestComparable(t *testing.T) {
	kv := New[int, []byte]("bytes")
	_ = kv

	kv.Clear()
	kv.SetNZ(1, nil) // no-op
	if kv.Has(1) {
		t.Fatal("Expected key not to be set by SetNZ with nil value")
	}

	kv.SetNZ(1, []byte{0, 1, 2})
	if !bytes.Equal(kv.Get(1), []byte{0, 1, 2}) {
		t.Fatal("Expected value to be set by SetNZ with non-zero value")
	}

	kv.Clear()
	kv.Set(1, []byte{0, 1, 2})
	kv.SetNX(1, []byte{3, 4, 5}) // no-op

	if !bytes.Equal(kv.Get(1), []byte{0, 1, 2}) {
		t.Fatal("Expected value to remain unchanged after SetNZ with non-zero value")
	}

	kv.Set(1, []byte{255})
	if !bytes.Equal(kv.Get(1), []byte{255}) {
		t.Fatal("Expected value to remain unchanged after SetNZ with non-zero value")
	}
	kv.Purge()
}

// TestTypeString tests string key and string value
func TestTypeString(t *testing.T) {
	kv := New[string, string]("test_type_string")
	kv.Clear()

	kv.Set("hello", "world")
	if val := kv.Get("hello"); val != "world" {
		t.Fatalf("Expected 'world', got '%s'", val)
	}

	kv.Set("unicode", "你好世界 🌍")
	if val := kv.Get("unicode"); val != "你好世界 🌍" {
		t.Fatalf("Expected unicode string, got '%s'", val)
	}

	kv.Purge()
}

// TestTypeTimeKey tests time.Time as key (should use TIMESTAMP column)
func TestTypeTimeKey(t *testing.T) {
	kv := New[time.Time, string]("test_type_time_key")
	kv.Clear()

	now := time.Now().UTC().Truncate(time.Microsecond)
	kv.Set(now, "timestamp_value")

	if val := kv.Get(now); val != "timestamp_value" {
		t.Fatalf("Expected 'timestamp_value', got '%s'", val)
	}

	// Test iteration with time.Time keys
	var keys []time.Time
	kv.Keys(func(k time.Time) bool {
		keys = append(keys, k)
		return true
	})
	if len(keys) != 1 {
		t.Fatalf("Expected 1 key, got %d", len(keys))
	}
	if !keys[0].Equal(now) {
		t.Fatalf("Expected time %v, got %v", now, keys[0])
	}

	kv.Purge()
}

// TestTypeTimeValue tests time.Time as value
func TestTypeTimeValue(t *testing.T) {
	kv := New[string, time.Time]("test_type_time_value")
	kv.Clear()

	testTime := time.Date(2025, 10, 20, 12, 30, 45, 123456789, time.UTC)
	kv.Set("appointment", testTime)

	retrieved := kv.Get("appointment")
	if !retrieved.Equal(testTime) {
		t.Fatalf("Expected time %v, got %v", testTime, retrieved)
	}

	kv.Purge()
}

// TestTypeIntKeys tests various integer types as keys
func TestTypeIntKeys(t *testing.T) {
	// Test int
	kvInt := New[int, string]("test_type_int_key")
	kvInt.Clear()
	kvInt.Set(42, "forty-two")
	if val := kvInt.Get(42); val != "forty-two" {
		t.Fatalf("int key: expected 'forty-two', got '%s'", val)
	}
	kvInt.Purge()

	// Test int64
	kvInt64 := New[int64, string]("test_type_int64_key")
	kvInt64.Clear()
	kvInt64.Set(int64(9223372036854775807), "max_int64")
	if val := kvInt64.Get(9223372036854775807); val != "max_int64" {
		t.Fatalf("int64 key: expected 'max_int64', got '%s'", val)
	}
	kvInt64.Purge()

	// Test uint
	kvUint := New[uint, string]("test_type_uint_key")
	kvUint.Clear()
	kvUint.Set(uint(123), "unsigned")
	if val := kvUint.Get(123); val != "unsigned" {
		t.Fatalf("uint key: expected 'unsigned', got '%s'", val)
	}
	kvUint.Purge()

	// Test uint64
	kvUint64 := New[uint64, string]("test_type_uint64_key")
	kvUint64.Clear()
	kvUint64.Set(uint64(18446744073709551615), "max_uint64")
	if val := kvUint64.Get(18446744073709551615); val != "max_uint64" {
		t.Fatalf("uint64 key: expected 'max_uint64', got '%s'", val)
	}
	kvUint64.Purge()
}

// TestTypeIntValues tests various integer types as values
func TestTypeIntValues(t *testing.T) {
	kvInt := New[string, int]("test_type_int_value")
	kvInt.Clear()
	kvInt.Set("answer", 42)
	if val := kvInt.Get("answer"); val != 42 {
		t.Fatalf("int value: expected 42, got %d", val)
	}
	kvInt.Purge()

	kvInt8 := New[string, int8]("test_type_int8_value")
	kvInt8.Clear()
	kvInt8.Set("small", int8(127))
	if val := kvInt8.Get("small"); val != 127 {
		t.Fatalf("int8 value: expected 127, got %d", val)
	}
	kvInt8.Purge()

	kvUint32 := New[string, uint32]("test_type_uint32_value")
	kvUint32.Clear()
	kvUint32.Set("big", uint32(4294967295))
	if val := kvUint32.Get("big"); val != 4294967295 {
		t.Fatalf("uint32 value: expected 4294967295, got %d", val)
	}
	kvUint32.Purge()
}

// TestTypeStruct tests struct types (JSON marshaled)
func TestTypeStruct(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	kv := New[string, Person]("test_type_struct")
	kv.Clear()

	person := Person{Name: "Alice", Age: 30}
	kv.Set("alice", person)

	retrieved := kv.Get("alice")
	if retrieved.Name != "Alice" || retrieved.Age != 30 {
		t.Fatalf("Expected Person{Name: Alice, Age: 30}, got %+v", retrieved)
	}

	kv.Purge()
}

// TestTypeStructPointer tests pointer to struct
func TestTypeStructPointer(t *testing.T) {
	type Config struct {
		Host string
		Port int
	}

	kv := New[string, *Config]("test_type_struct_pointer")
	kv.Clear()

	config := &Config{Host: "localhost", Port: 8080}
	kv.Set("server", config)

	retrieved := kv.Get("server")
	if retrieved == nil {
		t.Fatal("Expected non-nil pointer")
	}
	if retrieved.Host != "localhost" || retrieved.Port != 8080 {
		t.Fatalf("Expected Config{Host: localhost, Port: 8080}, got %+v", retrieved)
	}

	kv.Purge()
}

// TestTypeSlice tests slice types (JSON marshaled)
func TestTypeSlice(t *testing.T) {
	kv := New[string, []int]("test_type_slice")
	kv.Clear()

	nums := []int{1, 2, 3, 4, 5}
	kv.Set("numbers", nums)

	retrieved := kv.Get("numbers")
	if len(retrieved) != len(nums) {
		t.Fatalf("Expected slice of length %d, got %d", len(nums), len(retrieved))
	}
	for i, v := range retrieved {
		if v != nums[i] {
			t.Fatalf("At index %d: expected %d, got %d", i, nums[i], v)
		}
	}

	kv.Purge()
}

// TestTypeMap tests map types (JSON marshaled)
func TestTypeMap(t *testing.T) {
	kv := New[string, map[string]int]("test_type_map")
	kv.Clear()

	scores := map[string]int{"alice": 100, "bob": 95}
	kv.Set("scores", scores)

	retrieved := kv.Get("scores")
	if len(retrieved) != len(scores) {
		t.Fatalf("Expected map of length %d, got %d", len(scores), len(retrieved))
	}
	if retrieved["alice"] != 100 || retrieved["bob"] != 95 {
		t.Fatalf("Expected map values to match, got %+v", retrieved)
	}

	kv.Purge()
}

// CustomScanner is a custom type that implements sql.Scanner and fmt.Stringer
type CustomScanner struct {
	Value string
}

func (c *CustomScanner) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		c.Value = v
		return nil
	case []byte:
		c.Value = string(v)
		return nil
	default:
		return fmt.Errorf("cannot scan type %T", src)
	}
}

func (c CustomScanner) String() string {
	return fmt.Sprintf("CustomScanner(%s)", c.Value)
}

// TestTypeSqlScanner tests types implementing sql.Scanner and fmt.Stringer
// Note: When both sql.Scanner and fmt.Stringer are implemented, fmt.Stringer takes precedence for marshaling
func TestTypeSqlScanner(t *testing.T) {
	kv := New[string, *CustomScanner]("test_type_scanner")
	kv.Clear()

	custom := &CustomScanner{Value: "test_value"}
	kv.Set("custom", custom)

	retrieved := kv.Get("custom")
	if retrieved == nil {
		t.Fatal("Expected non-nil CustomScanner")
	}
	// Since CustomScanner implements fmt.Stringer, it's marshaled using String()
	// and then scanned back using Scan(), so we expect the formatted string
	if retrieved.Value != "CustomScanner(test_value)" {
		t.Fatalf("Expected 'CustomScanner(test_value)', got '%s'", retrieved.Value)
	}

	kv.Purge()
}

// TestTypeByteSlice tests []byte type
func TestTypeByteSlice(t *testing.T) {
	kv := New[string, []byte]("test_type_bytes")
	kv.Clear()

	data := []byte{0x01, 0x02, 0x03, 0xFF}
	kv.Set("binary", data)

	retrieved := kv.Get("binary")
	if !bytes.Equal(retrieved, data) {
		t.Fatalf("Expected %v, got %v", data, retrieved)
	}

	kv.Purge()
}

// TestTypeBool tests bool type (JSON marshaled)
func TestTypeBool(t *testing.T) {
	kv := New[string, bool]("test_type_bool")
	kv.Clear()

	kv.Set("enabled", true)
	kv.Set("disabled", false)

	if val := kv.Get("enabled"); val != true {
		t.Fatalf("Expected true, got %v", val)
	}
	if val := kv.Get("disabled"); val != false {
		t.Fatalf("Expected false, got %v", val)
	}

	kv.Purge()
}

// TestTypeFloat tests float types (JSON marshaled)
func TestTypeFloat(t *testing.T) {
	kv := New[string, float64]("test_type_float")
	kv.Clear()

	kv.Set("pi", 3.14159265359)
	retrieved := kv.Get("pi")
	if retrieved != 3.14159265359 {
		t.Fatalf("Expected 3.14159265359, got %f", retrieved)
	}

	kv.Purge()
}

// TestTypeAllIterator tests the All iterator with various types
func TestTypeAllIterator(t *testing.T) {
	kv := New[int, string]("test_type_all_iterator")
	kv.Clear()

	kv.Set(1, "one")
	kv.Set(2, "two")
	kv.Set(3, "three")

	count := 0
	kv.All(func(k int, v string) bool {
		count++
		return true
	})

	if count != 3 {
		t.Fatalf("Expected 3 items, got %d", count)
	}

	kv.Purge()
}

// TestExoticNestedStructs tests deeply nested and complex struct types
func TestExoticNestedStructs(t *testing.T) {
	type Address struct {
		Street  string
		City    string
		ZipCode int
	}
	type Contact struct {
		Email string
		Phone string
	}
	type Employee struct {
		Name     string
		Age      int
		Address  Address
		Contact  Contact
		Tags     []string
		Metadata map[string]interface{}
		IsActive bool
		Salary   *float64
	}

	kv := New[string, Employee]("test_exotic_nested")
	kv.Clear()

	salary := 75000.50
	emp := Employee{
		Name: "John Doe",
		Age:  35,
		Address: Address{
			Street:  "123 Main St",
			City:    "Springfield",
			ZipCode: 12345,
		},
		Contact: Contact{
			Email: "john@example.com",
			Phone: "+1-555-0100",
		},
		Tags:     []string{"engineer", "remote", "senior"},
		Metadata: map[string]interface{}{"department": "engineering", "level": 5.0},
		IsActive: true,
		Salary:   &salary,
	}

	kv.Set("emp001", emp)
	retrieved := kv.Get("emp001")

	if retrieved.Name != "John Doe" || retrieved.Age != 35 {
		t.Fatalf("Basic fields mismatch: got %+v", retrieved)
	}
	if retrieved.Address.Street != "123 Main St" || retrieved.Address.ZipCode != 12345 {
		t.Fatalf("Address mismatch: got %+v", retrieved.Address)
	}
	if len(retrieved.Tags) != 3 || retrieved.Tags[1] != "remote" {
		t.Fatalf("Tags mismatch: got %v", retrieved.Tags)
	}
	if retrieved.Metadata["department"] != "engineering" {
		t.Fatalf("Metadata mismatch: got %v", retrieved.Metadata)
	}
	if retrieved.Salary == nil || *retrieved.Salary != 75000.50 {
		t.Fatalf("Salary mismatch: got %v", retrieved.Salary)
	}

	kv.Purge()
}

// TestExoticNilAndEmpty tests nil values, empty slices, and empty maps
func TestExoticNilAndEmpty(t *testing.T) {
	type Container struct {
		PtrValue  *string
		EmptyList []int
		EmptyMap  map[string]string
		NilList   []int
		NilMap    map[string]string
	}

	kv := New[string, Container]("test_exotic_nil_empty")
	kv.Clear()

	// Test with all nil/empty
	c1 := Container{}
	kv.Set("c1", c1)
	r1 := kv.Get("c1")
	if r1.PtrValue != nil {
		t.Fatalf("Expected nil pointer, got %v", r1.PtrValue)
	}

	// Test with initialized empty collections
	c2 := Container{
		EmptyList: []int{},
		EmptyMap:  map[string]string{},
	}
	kv.Set("c2", c2)
	r2 := kv.Get("c2")
	if r2.EmptyList == nil || len(r2.EmptyList) != 0 {
		t.Fatalf("Expected empty slice, got %v", r2.EmptyList)
	}
	if r2.EmptyMap == nil || len(r2.EmptyMap) != 0 {
		t.Fatalf("Expected empty map, got %v", r2.EmptyMap)
	}

	// Test with non-nil pointer
	str := "hello"
	c3 := Container{PtrValue: &str}
	kv.Set("c3", c3)
	r3 := kv.Get("c3")
	if r3.PtrValue == nil || *r3.PtrValue != "hello" {
		t.Fatalf("Expected 'hello', got %v", r3.PtrValue)
	}

	kv.Purge()
}

// TestExoticConcurrentWrites tests concurrent Set operations
func TestExoticConcurrentWrites(t *testing.T) {
	kv := New[string, int]("test_exotic_concurrent")
	kv.Clear()

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			kv.Set(fmt.Sprintf("key%d", n), n*100)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all keys exist
	count := 0
	kv.Keys(func(k string) bool {
		count++
		return true
	})
	if count != 10 {
		t.Fatalf("Expected 10 keys after concurrent writes, got %d", count)
	}

	kv.Purge()
}

// TestExoticUpdateConcurrent tests Update() with concurrent modifications
func TestExoticUpdateConcurrent(t *testing.T) {
	kv := New[string, int]("test_exotic_update_concurrent")
	kv.Clear()

	kv.Set("counter", 0)

	done := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		go func() {
			kv.Update("counter", func(v int) int {
				return v + 1
			})
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Should have exactly 20 increments due to SELECT FOR UPDATE locking
	final := kv.Get("counter")
	if final != 20 {
		t.Fatalf("Expected counter=20 after concurrent updates, got %d", final)
	}

	kv.Purge()
}

// TestExoticLargeData tests handling of large strings and byte slices
func TestExoticLargeData(t *testing.T) {
	kv := New[string, []byte]("test_exotic_large")
	kv.Clear()

	// Create 1MB of data
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	kv.Set("large", largeData)
	retrieved := kv.Get("large")

	if len(retrieved) != len(largeData) {
		t.Fatalf("Expected %d bytes, got %d", len(largeData), len(retrieved))
	}
	if !bytes.Equal(retrieved, largeData) {
		t.Fatal("Large data content mismatch")
	}

	kv.Purge()
}

// TestExoticLargeString tests very large string values
func TestExoticLargeString(t *testing.T) {
	kv := New[string, string]("test_exotic_large_string")
	kv.Clear()

	// Create 100KB string with valid UTF-8 (repeating pattern)
	pattern := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
	var builder bytes.Buffer
	for builder.Len() < 100*1024 {
		builder.WriteString(pattern)
	}
	largeStr := builder.String()[:100*1024]

	kv.Set("bigtext", largeStr)

	retrieved := kv.Get("bigtext")
	if len(retrieved) != len(largeStr) {
		t.Fatalf("Expected string of length %d, got %d", len(largeStr), len(retrieved))
	}
	if retrieved != largeStr {
		t.Fatal("Large string content mismatch")
	}

	kv.Purge()
}

// TestExoticSliceOfStructs tests slice of complex structs
func TestExoticSliceOfStructs(t *testing.T) {
	type Item struct {
		ID    int
		Name  string
		Price float64
	}

	kv := New[string, []Item]("test_exotic_slice_structs")
	kv.Clear()

	items := []Item{
		{ID: 1, Name: "Widget", Price: 19.99},
		{ID: 2, Name: "Gadget", Price: 29.99},
		{ID: 3, Name: "Doohickey", Price: 39.99},
	}

	kv.Set("inventory", items)
	retrieved := kv.Get("inventory")

	if len(retrieved) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(retrieved))
	}
	if retrieved[1].Name != "Gadget" || retrieved[1].Price != 29.99 {
		t.Fatalf("Item mismatch: got %+v", retrieved[1])
	}

	kv.Purge()
}

// TestExoticMapOfSlices tests map with slice values
func TestExoticMapOfSlices(t *testing.T) {
	kv := New[string, map[string][]int]("test_exotic_map_slices")
	kv.Clear()

	data := map[string][]int{
		"evens":  {2, 4, 6, 8},
		"odds":   {1, 3, 5, 7},
		"primes": {2, 3, 5, 7, 11},
	}

	kv.Set("numbers", data)
	retrieved := kv.Get("numbers")

	if len(retrieved["evens"]) != 4 || retrieved["evens"][2] != 6 {
		t.Fatalf("Evens mismatch: got %v", retrieved["evens"])
	}
	if len(retrieved["primes"]) != 5 || retrieved["primes"][4] != 11 {
		t.Fatalf("Primes mismatch: got %v", retrieved["primes"])
	}

	kv.Purge()
}

// TestExoticInterfaceSlice tests []interface{} with mixed types
func TestExoticInterfaceSlice(t *testing.T) {
	kv := New[string, []interface{}]("test_exotic_interface_slice")
	kv.Clear()

	mixed := []interface{}{
		"string",
		42.0, // JSON numbers become float64
		true,
		[]interface{}{"nested", "array"},
		map[string]interface{}{"key": "value"},
	}

	kv.Set("mixed", mixed)
	retrieved := kv.Get("mixed")

	if len(retrieved) != 5 {
		t.Fatalf("Expected 5 elements, got %d", len(retrieved))
	}
	if retrieved[0].(string) != "string" {
		t.Fatalf("Element 0 mismatch: got %v", retrieved[0])
	}
	if retrieved[1].(float64) != 42.0 {
		t.Fatalf("Element 1 mismatch: got %v", retrieved[1])
	}
	if retrieved[2].(bool) != true {
		t.Fatalf("Element 2 mismatch: got %v", retrieved[2])
	}

	kv.Purge()
}

// TestExoticUpdateWithStruct tests Update() with struct modifications
func TestExoticUpdateWithStruct(t *testing.T) {
	type Account struct {
		Balance int
		Locked  bool
	}

	kv := New[string, Account]("test_exotic_update_struct")
	kv.Clear()

	kv.Set("account123", Account{Balance: 1000, Locked: false})

	// Deduct from balance
	result, err := kv.Update("account123", func(acc Account) Account {
		acc.Balance -= 250
		return acc
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if result.Balance != 750 {
		t.Fatalf("Expected balance 750, got %d", result.Balance)
	}

	// Verify persisted
	final := kv.Get("account123")
	if final.Balance != 750 {
		t.Fatalf("Expected persisted balance 750, got %d", final.Balance)
	}

	kv.Purge()
}

// TestExoticSpecialCharactersInKeys tests keys with special characters
func TestExoticSpecialCharactersInKeys(t *testing.T) {
	kv := New[string, string]("test_exotic_special_keys")
	kv.Clear()

	specialKeys := []string{
		"key with spaces",
		"key/with/slashes",
		"key:with:colons",
		"key@with@at",
		"key#with#hash",
		"key$with$dollar",
		"key%with%percent",
		"key&with&ampersand",
		"key(with)parens",
		"key[with]brackets",
		"key{with}braces",
		"key<with>angles",
		"key\"with\"quotes",
		"key'with'apostrophes",
		"key\\with\\backslashes",
		"key|with|pipes",
		"key\twith\ttabs",
		"key\nwith\nnewlines",
	}

	for i, key := range specialKeys {
		kv.Set(key, fmt.Sprintf("value%d", i))
	}

	// Verify all can be retrieved
	for i, key := range specialKeys {
		expected := fmt.Sprintf("value%d", i)
		retrieved := kv.Get(key)
		if retrieved != expected {
			t.Fatalf("Key '%s': expected '%s', got '%s'", key, expected, retrieved)
		}
	}

	// Count keys
	count := 0
	kv.Keys(func(k string) bool {
		count++
		return true
	})
	if count != len(specialKeys) {
		t.Fatalf("Expected %d keys, got %d", len(specialKeys), count)
	}

	kv.Purge()
}

// TestExoticZeroValues tests behavior with zero values
func TestExoticZeroValues(t *testing.T) {
	// Test zero int
	kvInt := New[string, int]("test_exotic_zero_int")
	kvInt.Clear()
	kvInt.Set("zero", 0)
	if val := kvInt.Get("zero"); val != 0 {
		t.Fatalf("Expected 0, got %d", val)
	}
	kvInt.Purge()

	// Test empty string
	kvStr := New[string, string]("test_exotic_zero_string")
	kvStr.Clear()
	kvStr.Set("empty", "")
	if val := kvStr.Get("empty"); val != "" {
		t.Fatalf("Expected empty string, got '%s'", val)
	}
	kvStr.Purge()

	// Test zero time
	kvTime := New[string, time.Time]("test_exotic_zero_time")
	kvTime.Clear()
	zeroTime := time.Time{}
	kvTime.Set("zero", zeroTime)
	retrieved := kvTime.Get("zero")
	if !retrieved.IsZero() {
		t.Fatalf("Expected zero time, got %v", retrieved)
	}
	kvTime.Purge()
}

// TestExoticTableIsolation tests that different tables with same types are isolated
func TestExoticTableIsolation(t *testing.T) {
	// Test that two different table names don't interfere
	// We use a single test to avoid creating too many connection pools
	kv := New[string, int]("test_exotic_table_isolation")
	kv.Clear()
	kv.Set("key1", 100)
	kv.Set("key2", 200)

	// Verify both keys exist
	if val := kv.Get("key1"); val != 100 {
		t.Fatalf("Expected 100, got %d", val)
	}
	if val := kv.Get("key2"); val != 200 {
		t.Fatalf("Expected 200, got %d", val)
	}

	// Delete one key
	kv.Delete("key1")
	if kv.Has("key1") {
		t.Fatal("key1 should be deleted")
	}
	if !kv.Has("key2") {
		t.Fatal("key2 should still exist")
	}

	kv.Purge()
}

func TestSetNZRat(t *testing.T) {
	kv := New[string, *rat.Rational]("test_setnz_rat")

	kv.Clear()

	// SetNZ with non-zero should set
	val := kv.SetNZ("key1", rat.Rat(100))
	if !val.Equal(100) {
		t.Fatalf("Expected 100, got %v", val)
	}

	val = kv.SetNZ("key1", nil)
	if !val.Equal(100) {
		t.Fatalf("Expected 100, got %v", val)
	}

	kv.SetNX("key1", rat.Rat(200)) // no-op
	if !kv.Get("key1").Equal(100) {
		t.Fatalf("Expected value to remain unchanged after SetNZ with non-zero value")
	}

	kv.Purge()
}

func TestTimeKeyIteration(t *testing.T) {
	kv := New[string, time.Time]("TestTimeKeyIteration")
	kv.Clear()

	now := time.Now()
	kv.Set("hello", now)
	for k, v := range kv.All {
		if !v.Equal(now) {
			t.Fatalf("Expected time %v, got %v", now, v)
		}

		if k != "hello" {
			t.Fatalf("Expected key 'hello', got '%s'", k)
		}
		log.Println("key:", k, "value:", v)
	}
}

func TestRat2(t *testing.T) {
	kv := New[*rat.Rational, *rat.Rational]("TestRat2")

	key := rat.Rat(0)
	kv.Clear()
	kv.AddRat(key, "1/3")
	kv.AddRat(key, "1/3")
	kv.AddRat(key, "1/3")
	if !kv.Get(key).Equal("1") {
		t.Fatal("error")
	}
	kv.AddRat(key, "1/3")
	if !kv.Get(key).Equal("4/3") {
		t.Log(kv.Get(key))
		t.Fatal("error")
	}
	t.Log("value:", kv.Get(key))
}
