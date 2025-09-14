package message

import (
	"sync"
	"testing"
)

// TestGenerateGlobalIDConcurrentUniqueness 并发生成ID唯一性
// 验证CAS抢占毫秒时间戳 + 序列号递增在并发下不产生重复ID
func TestGenerateGlobalIDConcurrentUniqueness(t *testing.T) {
	g := NewGlobalIDGenerator()

	const goroutines = 32
	const idsPerGoroutine = 500

	seen := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(workerID uint8) {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id := g.GenerateGlobalID(uint32(j+1), uint32(workerID+1), workerID, 0)
				mu.Lock()
				key := id.String()
				if seen[key] {
					t.Errorf("duplicate ID generated: %s", key)
				}
				seen[key] = true
				mu.Unlock()
			}
		}(uint8(i))
	}

	wg.Wait()

	expected := goroutines * idsPerGoroutine
	if len(seen) != expected {
		t.Errorf("expected %d unique IDs, got %d", expected, len(seen))
	}
}

// TestGenerateGlobalIDMonotonic 验证ID时间戳单调不减
// 即时钟回拨防护：后续生成的ID时间戳不会小于之前的
func TestGenerateGlobalIDMonotonic(t *testing.T) {
	g := NewGlobalIDGenerator()

	lastTimestamp := int64(0)
	for i := 0; i < 1000; i++ {
		id := g.GenerateGlobalID(1, 1, 1, 0)
		ts := id.ExtractTimestamp()
		if ts < lastTimestamp {
			t.Fatalf("timestamp went backwards: %d < %d at iteration %d", ts, lastTimestamp, i)
		}
		lastTimestamp = ts
	}
}

// TestGenerateGlobalIDFieldExtraction 验证ID各字段编解码往返
func TestGenerateGlobalIDFieldExtraction(t *testing.T) {
	g := NewGlobalIDGenerator()

	userID := uint32(123456)
	roomID := uint32(654321)
	loginID := uint8(5)
	custom := uint8(3)

	id := g.GenerateGlobalID(userID, roomID, loginID, custom)

	if got := id.ExtractUserID(); got != userID {
		t.Errorf("userID roundtrip failed: got %d, want %d", got, userID)
	}
	if got := id.ExtractRoomID(); got != roomID {
		t.Errorf("roomID roundtrip failed: got %d, want %d", got, roomID)
	}
	if got := id.ExtractLoginID(); got != loginID {
		t.Errorf("loginID roundtrip failed: got %d, want %d", got, loginID)
	}
	if got := id.ExtractCustom(); got != custom {
		t.Errorf("custom roundtrip failed: got %d, want %d", got, custom)
	}
	if !id.IsValid() {
		t.Error("generated ID should be valid")
	}
}
