package cache

import (
	"sync"
	"time"
)

const (
	Inc  = iota
	Dec  = iota
	Stop = iota
)

type Count struct {
	v  int
	op chan int
}

func NewCount() *Count {
	c := &Count{
		v:  0,
		op: make(chan int, 5),
		// vc: make(chan int, 1),
	}

	go func() {
		for {
			op := <-c.op
			switch op {
			case Inc:
				c.v++
			case Dec:
				c.v--
			case Stop:
				return
			}
		}
	}()
	return c
}

func (c *Count) Value() int {
	return c.v
}

func (c *Count) Inc() {
	c.op <- Inc
}

func (c *Count) Dec() {
	c.op <- Dec
}

type Value struct {
	V          interface{}
	expiration time.Time
}

type Counter struct {
	CacheSeconds time.Duration
	CacheThresh  int
	c            map[string]*Count
	clock        *sync.Mutex
}

func NewCounter(CacheSeconds, CacheThresh int) *Counter {
	return &Counter{
		CacheSeconds: time.Second * time.Duration(CacheSeconds),
		CacheThresh:  CacheThresh,
		c:            make(map[string]*Count),
		clock:        &sync.Mutex{},
	}
}

func (c *Counter) Hit(key string) bool {
	c.clock.Lock()
	if _, ok := c.c[key]; !ok {
		c.c[key] = NewCount()
	}
	c.clock.Unlock()

	c.c[key].Inc()
	go func() {
		<-time.After(c.CacheSeconds)
		c.c[key].Dec()
	}()

	return c.c[key].Value() >= c.CacheThresh
}

type TickMap struct {
	m     map[string]*Value
	mlock *sync.Mutex
}

func NewTickMap() *TickMap {
	return &TickMap{
		m:     make(map[string]*Value),
		mlock: &sync.Mutex{},
	}
}

func (c *TickMap) Get(key string) (interface{}, bool) {
	c.mlock.Lock()
	vs, ok := c.m[key]
	c.mlock.Unlock()
	var val interface{}
	if !ok {
		val = nil
	} else if time.Now().Before(vs.expiration) {
		val = vs.V
	} else {
		val = nil
		ok = false
	}

	return val, ok
}

func (c *TickMap) Put(key string, value interface{}, validSecs int) {
	d := time.Duration(validSecs) * time.Second
	exp := time.Now().Add(d)
	c.mlock.Lock()
	go func() {
		cc := *c
		<-time.After(d)
		cc.mlock.Lock()
		if cc.m[key].expiration == exp {
			delete(cc.m, key)
		}
		cc.mlock.Unlock()
	}()
	c.m[key] = &Value{
		V:          value,
		expiration: exp,
	}
	c.mlock.Unlock()
}

func (c *TickMap) Delete(key string) {
	c.mlock.Lock()
	delete(c.m, key)
	c.mlock.Unlock()
}

// return all the keys
func (c *TickMap) Freeze() []string {
	c.mlock.Lock()
	defer c.mlock.Unlock()
	s := make([]string, len(c.m))
	i := 0
	for key, _ := range c.m {
		s[i] = key
		i++
	}
	return s
}

func (c *TickMap) Clear() {
	c = NewTickMap()
}

type Cache struct {
	c *Counter
	m *TickMap
}

func NewCache(CacheSeconds, CacheThresh int) *Cache {
	return &Cache{
		m: NewTickMap(),
		c: NewCounter(CacheSeconds, CacheThresh),
	}
}

// returns the value if exists, also returns whether a leaase
// is wanted
func (c *Cache) Get(key string) (interface{}, bool, bool) {
	val, ok := c.m.Get(key)
	lease := c.c.Hit(key)
	return val, ok, lease
}

func (c *Cache) Put(key string, value interface{}, validSecs int) {
	c.m.Put(key, value, validSecs)
}

func (c *Cache) Delete(key string) {
	c.m.Delete(key)
}
