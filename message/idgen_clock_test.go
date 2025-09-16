package message

import (
	"sync"
	"testing"
)

// TestGenerateGlobalIDConcurrentUniqueness 并发生成ID唯一性
// 使用相同的(user,room,login,custom)键并发压测——
// 只有每毫秒的序号位图占位分配正确，才能保证不重复
func TestGenerateGlobalIDConcurrentUniqueness(t *testing.T) {
	g := NewGlobalIDGenerator()

	const goroutines = 32
	const idsPerGoroutine = 200

	seen := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				// 所有协程使用相同的key，任何序号分配错误都会撞出重复
				id := g.GenerateGlobalID(1, 1, 1, 0)
				mu.Lock()
				key := id.String()
				if seen[key] {
					t.Errorf("duplicate ID generated: %s", key)
				}
				seen[key] = true
				mu.Unlock()
			}
		}()
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
