package pgmap

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestTypeConversion verifies that all supported types are correctly stored and retrieved
// with their appropriate PostgreSQL column types
func TestTypeConversion(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		kv := New[string, string]("test_type_string")
		defer kv.Purge()

		// Verify column type
		verifyColumnType(t, kv.tableName, "key", "text")
		verifyColumnType(t, kv.tableName, "value", "text")

		// Test storage and retrieval
		kv.Set("hello", "world")
		if got := kv.Get("hello"); got != "world" {
			t.Errorf("Expected 'world', got '%s'", got)
		}
	})

	t.Run("ByteSlice", func(t *testing.T) {
		kv := New[string, []byte]("test_type_bytes")
		defer kv.Purge()

		// Verify column type
		verifyColumnType(t, kv.tableName, "value", "bytea")

		// Test storage and retrieval
		data := []byte{0x01, 0x02, 0x03, 0xFF}
		kv.Set("binary", data)
		if got := kv.Get("binary"); !bytes.Equal(got, data) {
			t.Errorf("Expected %v, got %v", data, got)
		}
	})

	t.Run("Bool", func(t *testing.T) {
		kv := New[string, bool]("test_type_bool")
		defer kv.Purge()

		// Verify column type
		verifyColumnType(t, kv.tableName, "value", "boolean")

		// Test storage and retrieval
		kv.Set("flag", true)
		if got := kv.Get("flag"); got != true {
			t.Errorf("Expected true, got %v", got)
		}
		kv.Set("flag", false)
		if got := kv.Get("flag"); got != false {
			t.Errorf("Expected false, got %v", got)
		}
	})

	t.Run("IntTypes", func(t *testing.T) {
		// Test various integer types
		t.Run("Int", func(t *testing.T) {
			kv := New[string, int]("test_type_int")
			defer kv.Purge()
			verifyColumnType(t, kv.tableName, "value", "bigint")
			kv.Set("num", 42)
			if got := kv.Get("num"); got != 42 {
				t.Errorf("Expected 42, got %d", got)
			}
		})

		t.Run("Int64", func(t *testing.T) {
			kv := New[string, int64]("test_type_int64")
			defer kv.Purge()
			verifyColumnType(t, kv.tableName, "value", "bigint")
			kv.Set("num", int64(9223372036854775807))
			if got := kv.Get("num"); got != 9223372036854775807 {
				t.Errorf("Expected max int64, got %d", got)
			}
		})

		t.Run("Uint", func(t *testing.T) {
			kv := New[string, uint]("test_type_uint")
			defer kv.Purge()
			verifyColumnType(t, kv.tableName, "value", "bigint")
			kv.Set("num", uint(100))
			if got := kv.Get("num"); got != 100 {
				t.Errorf("Expected 100, got %d", got)
			}
		})
	})

	t.Run("FloatTypes", func(t *testing.T) {
		t.Run("Float32", func(t *testing.T) {
			kv := New[string, float32]("test_type_float32")
			defer kv.Purge()
			verifyColumnType(t, kv.tableName, "value", "double precision")
			kv.Set("pi", float32(3.14159))
			if got := kv.Get("pi"); got < 3.14 || got > 3.15 {
				t.Errorf("Expected ~3.14159, got %f", got)
			}
		})

		t.Run("Float64", func(t *testing.T) {
			kv := New[string, float64]("test_type_float64")
			defer kv.Purge()
			verifyColumnType(t, kv.tableName, "value", "double precision")
			kv.Set("e", 2.718281828459045)
			if got := kv.Get("e"); got < 2.71 || got > 2.72 {
				t.Errorf("Expected ~2.718, got %f", got)
			}
		})
	})

	t.Run("Time", func(t *testing.T) {
		kv := New[string, time.Time]("test_type_time")
		defer kv.Purge()

		// Verify column type (stored as BIGINT - UnixNano)
		verifyColumnType(t, kv.tableName, "value", "bigint")

		// Test storage and retrieval
		now := time.Now()
		kv.Set("timestamp", now)
		retrieved := kv.Get("timestamp")

		// Compare with nanosecond precision
		if !retrieved.Equal(now) {
			t.Errorf("Expected %v, got %v", now, retrieved)
		}

		// Test zero time
		zero := time.Time{}
		kv.Set("zero", zero)
		retrievedZero := kv.Get("zero")
		if !retrievedZero.IsZero() {
			t.Errorf("Expected zero time, got %v", retrievedZero)
		}
	})

	t.Run("Struct", func(t *testing.T) {
		type Person struct {
			Name string
			Age  int
		}

		kv := New[string, Person]("test_type_struct")
		defer kv.Purge()

		// Verify column type (structs are stored as JSON)
		verifyColumnType(t, kv.tableName, "value", "json")

		// Test storage and retrieval
		person := Person{Name: "Alice", Age: 30}
		kv.Set("person1", person)
		retrieved := kv.Get("person1")

		if retrieved.Name != "Alice" || retrieved.Age != 30 {
			t.Errorf("Expected Person{Name: Alice, Age: 30}, got %+v", retrieved)
		}
	})

	t.Run("StructPointer", func(t *testing.T) {
		type Address struct {
			Street string
			City   string
			Zip    int
		}

		kv := New[string, *Address]("test_type_struct_pointer")
		defer kv.Purge()

		// Verify column type (struct pointers are stored as JSON)
		verifyColumnType(t, kv.tableName, "value", "json")

		// Test storage and retrieval
		addr := &Address{Street: "123 Main St", City: "Springfield", Zip: 12345}
		kv.Set("addr1", addr)
		retrieved := kv.Get("addr1")

		if retrieved == nil {
			t.Fatal("Expected non-nil pointer")
		}
		if retrieved.Street != "123 Main St" || retrieved.City != "Springfield" || retrieved.Zip != 12345 {
			t.Errorf("Expected Address{Street: 123 Main St, City: Springfield, Zip: 12345}, got %+v", retrieved)
		}

		// Test nil pointer
		kv.Set("addr2", nil)
		retrievedNil := kv.Get("addr2")
		if retrievedNil != nil {
			t.Errorf("Expected nil pointer, got %+v", retrievedNil)
		}
	})

	t.Run("NestedStruct", func(t *testing.T) {
		type Contact struct {
			Email string
			Phone string
		}
		type Employee struct {
			Name    string
			Contact Contact
			Active  bool
		}

		kv := New[string, Employee]("test_type_nested_struct")
		defer kv.Purge()

		// Verify column type
		verifyColumnType(t, kv.tableName, "value", "json")

		// Test storage and retrieval
		emp := Employee{
			Name:    "Bob",
			Contact: Contact{Email: "bob@example.com", Phone: "555-1234"},
			Active:  true,
		}
		kv.Set("emp1", emp)
		retrieved := kv.Get("emp1")

		if retrieved.Name != "Bob" || retrieved.Contact.Email != "bob@example.com" || !retrieved.Active {
			t.Errorf("Expected nested struct, got %+v", retrieved)
		}
	})

	t.Run("SliceStruct", func(t *testing.T) {
		type Tag struct {
			Name  string
			Value string
		}

		kv := New[string, []Tag]("test_type_slice_struct")
		defer kv.Purge()

		// Verify column type
		verifyColumnType(t, kv.tableName, "value", "json")

		// Test storage and retrieval
		tags := []Tag{
			{Name: "env", Value: "prod"},
			{Name: "region", Value: "us-west"},
		}
		kv.Set("tags1", tags)
		retrieved := kv.Get("tags1")

		if len(retrieved) != 2 || retrieved[0].Name != "env" || retrieved[1].Value != "us-west" {
			t.Errorf("Expected slice of structs, got %+v", retrieved)
		}
	})

	t.Run("MapType", func(t *testing.T) {
		kv := New[string, map[string]int]("test_type_map")
		defer kv.Purge()

		// Verify column type
		verifyColumnType(t, kv.tableName, "value", "json")

		// Test storage and retrieval
		data := map[string]int{"a": 1, "b": 2, "c": 3}
		kv.Set("map1", data)
		retrieved := kv.Get("map1")

		if len(retrieved) != 3 || retrieved["a"] != 1 || retrieved["c"] != 3 {
			t.Errorf("Expected map, got %+v", retrieved)
		}
	})
}

// verifyColumnType checks the PostgreSQL column type
func verifyColumnType(t *testing.T, tableName, columnName, expectedType string) {
	t.Helper()

	kv := New[string, string]("_verify_helper")
	defer func() {
		// Don't purge, we're just using it to query
	}()

	query := `
		SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		WHERE c.relname = $1 AND a.attname = $2 AND n.nspname = 'public'
	`

	var actualType string
	ctx := context.Background()
	err := kv.db.QueryRow(ctx, query, tableName, columnName).Scan(&actualType)
	if err != nil {
		t.Fatalf("Failed to query column type: %v", err)
	}

	// Normalize type names
	normalizeType := func(t string) string {
		switch t {
		case "text", "character varying":
			return "text"
		case "bytea":
			return "bytea"
		case "boolean":
			return "boolean"
		case "bigint", "integer":
			return "bigint"
		case "double precision":
			return "double precision"
		case "json", "jsonb":
			return "json"
		default:
			return t
		}
	}

	if normalizeType(actualType) != normalizeType(expectedType) {
		t.Errorf("Column %s.%s: expected type %s, got %s", tableName, columnName, expectedType, actualType)
	}
}
