package elasticqueue

import (
	"sync"
	"testing"
	"time"

	"github.com/mixer/clock"
	"github.com/stretchr/testify/assert"
)

type conditionTest struct {
	Condition

	mu     sync.Mutex
	writes int
	closer func()
}

func NewConditionTest(c Condition) *conditionTest {
	ch, cancel := c.Write()
	test := &conditionTest{Condition: c, closer: cancel}

	go func() {
		for range ch {
			test.mu.Lock()
			test.writes++
			test.mu.Unlock()
		}
	}()

	return test
}

func (c *conditionTest) Writes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func (c *conditionTest) WaitForWrites(i int) int {
	deadline := time.Now().Add(time.Millisecond * 100)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if c.writes < i {
			c.mu.Unlock()
			time.Sleep(time.Millisecond * 2)
			continue
		}

		writes := c.writes
		c.mu.Unlock()
		return writes
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func (c *conditionTest) Stop() { c.closer() }

func TestWriteAfterIdle(t *testing.T) {
	clock := clock.NewMockClock()
	c := WriteAfterIdle(time.Millisecond * 50)
	c.(*afterIdleCondition).timer = clock.NewTimer(maxDuration)

	test := NewConditionTest(c)
	defer test.Stop()

	t.Run("ShouldNotWriteInitially", func(t *testing.T) {
		clock.AddTime(time.Millisecond * 50)
		assert.Equal(t, 0, test.WaitForWrites(1))
	})

	t.Run("ShouldWriteDataWhenIdle", func(t *testing.T) {
		c.Inserted(nil, 1)
		clock.AddTime(time.Millisecond * 50)
		assert.Equal(t, 1, test.WaitForWrites(1))
	})

	t.Run("ShouldNotWriteWithNoData", func(t *testing.T) {
		clock.AddTime(time.Millisecond * 50)
		assert.Equal(t, 1, test.WaitForWrites(2))
	})

	t.Run("ShouldNotWriteAfterMoreEvents", func(t *testing.T) {
		c.Inserted(nil, 1)
		clock.AddTime(time.Millisecond * 20)
		c.Inserted(nil, 2)
		clock.AddTime(time.Millisecond * 40)
		assert.Equal(t, 1, test.WaitForWrites(2))
	})
}

func TestWriteAfterInterval(t *testing.T) {
	clock := clock.NewMockClock()
	c := WriteAfterInterval(time.Millisecond * 50)
	c.(*afterIntervalCondition).timer = clock.NewTimer(maxDuration)

	test := NewConditionTest(c)
	defer test.Stop()

	t.Run("ShouldNotWriteInitially", func(t *testing.T) {
		clock.AddTime(time.Millisecond * 50)
		assert.Equal(t, 0, test.WaitForWrites(1)) // should not write anything initially
	})

	t.Run("ShouldWriteAfterGettingEvents", func(t *testing.T) {
		c.Inserted(nil, 1)
		clock.AddTime(time.Millisecond * 20)
		c.Inserted(nil, 2)
		clock.AddTime(time.Millisecond * 40)
		assert.Equal(t, 1, test.WaitForWrites(1))
	})

	t.Run("ShouldNotWriteWithNoData", func(t *testing.T) {
		clock.AddTime(time.Millisecond * 50)
		assert.Equal(t, 1, test.WaitForWrites(2))
	})
}

func TestWriteAfterLength(t *testing.T) {
	c := WriteAfterLength(2)
	assert.False(t, c.Inserted(nil, 1))
	assert.True(t, c.Inserted(nil, 2))
}

func TEstWriteAfterSize(t *testing.T) {
	c := WriterAfterByteSize(3)
	assert.False(t, c.Inserted([]byte{1}, 1))
	assert.False(t, c.Inserted([]byte{1}, 1))
	assert.True(t, c.Inserted([]byte{1}, 1))
	c.Flushed()
	assert.False(t, c.Inserted([]byte{1, 2}, 1))
	assert.True(t, c.Inserted([]byte{1}, 1))
}
