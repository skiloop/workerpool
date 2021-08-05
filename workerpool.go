package workerpool

import (
	"errors"
	"runtime"
	"sync"
	"time"
)

type WorkHandler func(p interface{}) error
type Logger interface {
	Print(i ...interface{})
	Printf(format string, args ...interface{})
	Debug(i ...interface{})
	Debugf(format string, args ...interface{})
	Info(i ...interface{})
	Infof(format string, args ...interface{})
	Warn(i ...interface{})
	Warnf(format string, args ...interface{})
	Error(i ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(i ...interface{})
	Fatalf(format string, args ...interface{})
	Panic(i ...interface{})
	Panicf(format string, args ...interface{})
}
type WorkerPool struct {
	Handler WorkHandler

	MaxWorkersCount int
	workersCount    int

	MaxIdleWorkerDuration time.Duration

	lock   sync.Mutex
	logger Logger

	stopCh   chan struct{}
	mustStop bool
	ready    []*workerChan

	workerChanPool sync.Pool
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan interface{}
}

var workerChanCap = func() int {
	// Use blocking workerChan if GOMAXPROCS=1.
	// This immediately switches Serve to WorkerFunc, which results
	// in higher performance (under go1.5 at least).
	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}

	// Use non-blocking workerChan if GOMAXPROCS>1,
	// since otherwise the Serve caller (Acceptor) may lag accepting
	// new connections if WorkerFunc is CPU-bound.
	return 1
}()

func (wp *WorkerPool) Stop() error {
	if wp.stopCh == nil {
		return errors.New("worker pool not started yet")
	}
	close(wp.stopCh)
	wp.stopCh = nil

	// Stop all the workers waiting for incoming connections.
	// Do not wait for busy workers - they will stop after
	// serving the connection and noticing wp.mustStop = true.
	wp.lock.Lock()
	defer wp.lock.Unlock()
	ready := wp.ready
	for i, ch := range ready {
		ch.ch <- nil
		ready[i] = nil
	}
	wp.ready = ready[:0]
	wp.mustStop = true
	return nil
}

func (wp *WorkerPool) Start() error {
	if wp.stopCh != nil {
		return errors.New("worker pool already started")
	}
	wp.stopCh = make(chan struct{})
	stopCh := wp.stopCh
	go func() {
		var scratch []*workerChan
		for {
			wp.clean(&scratch)
			select {
			case <-stopCh:
				return
			default:
				time.Sleep(wp.getMaxIdleWorkerDuration())
			}
		}
	}()
	return nil
}

func (wp *WorkerPool) getMaxIdleWorkerDuration() time.Duration {
	if wp.MaxIdleWorkerDuration <= 0 {
		return 10 * time.Second
	}
	return wp.MaxIdleWorkerDuration
}

func (wp *WorkerPool) clean(scratch *[]*workerChan) {
	maxIdleWorkerDuration := wp.getMaxIdleWorkerDuration()
	currentTime := time.Now()
	wp.lock.Lock()
	ready := wp.ready
	n := len(ready)
	i := 0
	for i < n && currentTime.Sub(ready[i].lastUseTime) > maxIdleWorkerDuration {
		i++
	}
	*scratch = append((*scratch)[:0], ready[:i]...)
	if i > 0 {
		m := copy(ready, ready[i:])
		for i = m; i < n; i++ {
			ready[i] = nil
		}
		wp.ready = ready[:m]
	}
	wp.lock.Unlock()

	// Notify obsolete workers to stop.
	// This notification must be outside the wp.lock, since ch.ch
	// may be blocking and may consume a lot of time if many workers
	// are located on non-local CPUs.
	tmp := *scratch
	for i, ch := range tmp {
		ch.ch <- nil
		tmp[i] = nil
	}
}

func (wp *WorkerPool) getCh() (ch *workerChan) {
	createWorker := false

	wp.lock.Lock()
	ready := wp.ready
	n := len(ready) - 1
	num := 0
	if n < 0 {
		if wp.workersCount < wp.MaxWorkersCount {
			createWorker = true
			wp.workersCount++
			num = wp.workersCount
		}
	} else {
		ch = ready[n]
		ready[n] = nil
		wp.ready = ready[:n]
	}
	wp.lock.Unlock()

	if ch == nil && createWorker {
		vch := wp.workerChanPool.Get()
		if vch == nil {
			vch = &workerChan{
				ch: make(chan interface{}, workerChanCap),
			}
		}
		ch = vch.(*workerChan)
		go func() {
			wp.workerFunc(ch, num)
			wp.workerChanPool.Put(vch)
		}()
	}
	return ch
}

func (wp *WorkerPool) Serve(i interface{}) bool {
	ch := wp.getCh()
	if ch == nil {
		return false
	}
	ch.ch <- i
	return true
}

func (wp *WorkerPool) workerFunc(ch *workerChan, n int) {
	var c interface{}
	var err error
	var count int64
	count = 0
	for c = range ch.ch {
		if c == nil {
			break
		}
		count++
		if err = wp.Handler(c); err != nil {
			c = nil
		}
		wp.logger.Debugf("[worker-%d] %d jobs done", n, count)
		if !wp.release(ch) {
			break
		}
	}

	wp.lock.Lock()
	wp.workersCount--
	wp.lock.Unlock()
}

func (wp *WorkerPool) release(ch *workerChan) bool {
	ch.lastUseTime = time.Now()
	wp.lock.Lock()
	defer wp.lock.Unlock()
	if wp.mustStop {
		return false
	}
	wp.ready = append(wp.ready, ch)
	return true
}

func NewWorkerPool(maxWorkers int, maxIdleDuration time.Duration, handler WorkHandler, logger Logger) *WorkerPool {
	return &WorkerPool{
		Handler:               handler,
		MaxWorkersCount:       maxWorkers,
		workersCount:          0,
		MaxIdleWorkerDuration: maxIdleDuration,
		mustStop:              false,
		logger:                logger,
	}
}
