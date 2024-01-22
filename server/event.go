package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	NoEvent         KeyEvent = iota
	Created                  // New key has been created
	Updated                  // Key value has been updated
	Deleted                  // Key has been deleted
	Expired                  // Key has expired
	Locked                   // Key has been locked, or re-locked
	Unlocked                 // Key has been unlocked
	Expunged                 // Key has pruned
	Accessed                 // Key value has been accessed via Get or Inspect
	LifespanSet              // When Lifespan is initially set, or changed
	LifespanRenewed          // When a lifespan is renewed via update, without changing the duration
)

// KeyEvent is an event that can occur with a key. These
// are attached to Event instances and available via
// [Server.WatchStream], [Server.WatchKeyValue], and
// [Server.Subscribe].
type KeyEvent uint

func (ke KeyEvent) GoString() string {
	return ke.String()
}

func (ke KeyEvent) String() string {
	switch ke {
	case Created:
		return "CREATED"
	case Updated:
		return "UPDATED"
	case Deleted:
		return "DELETED"
	case Expired:
		return "EXPIRED"
	case Locked:
		return "LOCKED"
	case Unlocked:
		return "UNLOCKED"
	case Expunged:
		return "EXPUNGED"
	case Accessed:
		return "ACCESSED"
	case LifespanSet:
		return "LIFESPAN_SET"
	case LifespanRenewed:
		return "LIFESPAN_RENEWED"
	case NoEvent:
		return "NO_EVENT"
	default:
		panic(fmt.Sprintf("unknown event: %d", ke))
	}
}

func (ke KeyEvent) LogValue() slog.Value {
	return slog.StringValue(ke.String())
}

// Event reflects an operation that has occurred with a key. Events are
// sent to [Server]'s `events` channel, and are then forwarded to
// [snapshotter], the event logger, and [eventStream], which forwards them
// to subscribers via eventWorker.
type Event struct {
	// Key is the key that the event is associated with
	Key string `json:"key"`

	// Event is the type of event that occurred
	Event KeyEvent `json:"event"`

	// Time is the time the event occurred
	Time time.Time `json:"time"`

	// ClientID is the ID of the client associated with the event.
	// If the event was triggered internally, this will be 'keyquarry'
	ClientID string `json:"client_id"`
}

func (e Event) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", e.Key),
		slog.String("event", e.Event.String()),
		slog.Time("time", e.Time),
		slog.String("client_id", e.ClientID),
	)
}

// eventWorker receives events from the eventStream on
// eventWorker.in, and forwards them to the subscriber on
// eventWorker.out
type eventWorker struct {
	// id is a hex-encoded 6-byte random string to differentiate
	// different instances of the same worker name
	id string

	// name is a name to attach to this worker, usually the
	// client_id of the subscriber, with a stream type appended
	name string

	// includeEvents is a list of events to forward to the subscriber.
	// If nil, all events will be forwarded.
	includeEvents []KeyEvent

	// includeKeys limits the events sent to the subscriber to only
	// the keys in this list. If nil, all keys will be forwarded.
	includeKeys []string

	// out is the channel the worker will re-broadcast events to
	out chan Event

	// in is the channel the worker will receive events from, which are
	// then re-broadcast to the out channel
	in chan Event

	// done is the channel the worker will receive a signal on when it
	// should stop broadcasting events on out
	done chan struct{}

	// stopped is the channel the worker will send a signal on when its
	// in channel has been closed or a signal has been received on done
	stopped chan struct{}

	// sendTimeout is the amount of time to wait for an event to be sent, so
	// messages on out don't block forever
	sendTimeout time.Duration

	// published is the number of events published by this worker
	published atomic.Uint64

	// timeouts is the number of events that timed out when sending
	timeouts atomic.Uint64

	running bool
	es      *eventStream
	logger  *slog.Logger
	mu      sync.RWMutex
}

// newEventWorker initializes a new [eventWorker] and returns it.
//   - name: the name of the subscriber
//   - includeEvents: a list of events to forward to the subscriber. If nil,
//     all events will be forwarded.
//   - includeKeys: a list of keys to forward to the subscriber. If nil, all
//     keys will be forwarded.
func newEventWorker(
	es *eventStream,
	name string,
	includeEvents []KeyEvent,
	includeKeys []string,
) *eventWorker {
	id := make([]byte, 12/2)
	if _, idErr := rand.Read(id); idErr != nil {
		panic(fmt.Sprintf("error creating worker ID: %s", idErr.Error()))
	}
	workerID := hex.EncodeToString(id)
	w := &eventWorker{
		id:            workerID,
		name:          name,
		out:           make(chan Event, es.bufferSize),
		in:            make(chan Event, es.bufferSize),
		done:          make(chan struct{}, 1),
		stopped:       make(chan struct{}, 1),
		includeEvents: includeEvents,
		includeKeys:   includeKeys,
		es:            es,
	}
	w.logger = es.logger.With(
		slog.String("subscriber", name),
		slog.String("logger", "event_worker"),
		slog.Group(
			"worker", slog.String("name", w.name),
			slog.String("id", w.id),
			slog.Uint64("buffer_size", es.bufferSize),
		),
	)
	return w
}

func (w *eventWorker) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", w.id),
		slog.String("name", w.name),
	)
}

// Run starts the worker, which will forward events to the subscriber.
// It will panic if called more than once.
func (w *eventWorker) Run(ctx context.Context) {
	sendTimeout := w.es.sendTimeout
	w.logger.Log(
		ctx,
		LevelNotice,
		"started event worker",
		"send_timeout",
		sendTimeout,
	)
	w.mu.Lock()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	w.es.srv.numEventSubscribers.Add(1)
	w.mu.Unlock()
	w.logger.Info("started", "send_timeout", sendTimeout)
	tracer := otel.Tracer("event_worker")

EventLoop:
	for {
		select {
		case <-wctx.Done():
			break EventLoop
		case <-w.done:
			cancel()
			break EventLoop
		// case event, ok := <-w.in:
		case event := <-w.in:
			switch {
			// case !ok:
			// 	break EventLoop
			case w.includeEvents != nil && !sliceContains(
				w.includeEvents,
				event.Event,
			):
				continue EventLoop
			case w.includeKeys != nil && !sliceContains(
				w.includeKeys,
				event.Key,
			):
				continue EventLoop
			}

			_, eventSpan := tracer.Start(
				wctx,
				"event",
				trace.WithNewRoot(),
				trace.WithAttributes(
					attribute.String("event.key", event.Key),
					attribute.String("event.time", event.Time.String()),
					attribute.String("event.client_id", event.ClientID),
					attribute.String("event.event", event.Event.String()),
					attribute.String("worker.name", w.name),
					attribute.String("worker.id", w.id),
				),
			)
			w.logger.Debug("saw event", "event", event)

			t := time.NewTimer(sendTimeout)
			select {
			case w.out <- event: // sent
				w.logger.Debug("worker sent event", "event", event)
				w.published.Add(1)
			case <-t.C: // timeout
				w.timeouts.Add(1)
				w.logger.Warn("event send timeout", "event", event)
			}
			eventSpan.End()
		}
	}

	close(w.out)
	w.stopped <- struct{}{}
	close(w.stopped)
	w.es.srv.numEventSubscribers.Add(decrementUint64)
	w.logger.Log(
		ctx,
		LevelNotice,
		"stopped event worker",
		"published", w.published.Load(),
		"timeouts", w.timeouts.Load(),
	)
}

// eventStream manages event workers, and forwards events from
// the event channel
type eventStream struct {
	// sendTimeout is the amount of time to wait for an event to be sent from
	// a worker to a subscriber, to avoid blocking.
	sendTimeout time.Duration

	// subscriberLimit sets a limit on the number of eventWorker instances
	// that can be running at the same time. 0=unlimited
	subscriberLimit uint64

	// bufferSize is the size of the buffer for the in/out event channels
	// when creating a new eventWorker
	bufferSize uint64

	// events is a channel receiving events from the main event channel
	events chan Event

	// workers maps the name provided in Subscribe, to a worker goroutine
	// responsible for forwarding events to the subscriber
	workers map[string]*eventWorker

	mu     sync.RWMutex
	logger *slog.Logger
	srv    *Server
}

// newEventStream initializes a new eventStream and returns it.
func newEventStream(srv *Server) *eventStream {
	ev := &eventStream{
		sendTimeout:     srv.cfg.EventStreamSendTimeout,
		subscriberLimit: srv.cfg.EventStreamSubscriberLimit,
		bufferSize:      srv.cfg.EventStreamBufferSize,
		logger:          srv.logger.With(loggerKey, "event_stream"),
		srv:             srv,
	}
	return ev
}

// Unsubscribe removes the subscriber/worker with the given
// name from registered workers, and stops the worker.
// A signal is sent on eventWorker.done channel, which causes
// the worker to close its outbound channel, and then send a signal
// on eventWorker.stopped. When that signal is received, the
// eventWorker.in channel is closed.
func (e *eventStream) Unsubscribe(name string) error {
	e.logger.Info("attempting to unsubscribe", "subscriber", name)

	e.mu.Lock()
	defer e.mu.Unlock()

	w, exists := e.workers[name]
	if !exists {
		e.logger.Info("subscriber does not exist", "subscriber", name)
		return fmt.Errorf("subscriber '%s' does not exist", name)
	}

	delete(e.workers, name)
	e.logger.Debug("stopping worker", "subscriber", name, "worker", w)

	w.done <- struct{}{}

	w.mu.Lock()
	defer w.mu.Unlock()

	e.logger.Log(
		context.Background(),
		LevelNotice,
		"unsubscribed",
		"subscriber_name",
		name,
		"worker",
		w,
	)
	e.logger.Debug("waiting for stop signal", "worker", w)
	<-w.stopped
	close(w.in)
	e.logger.Debug("got stop signal", "worker", w)
	return nil
}

// Subscribe creates a new eventWorker and returns a channel that will
// receive events. The channel will be closed when the subscriber is
// removed via Unsubscribe.
func (e *eventStream) Subscribe(
	ctx context.Context,
	name string, // subscriber name
	keys []string, // keys to subscribe to - leave empty to subscribe to all keys
	events []KeyEvent, // events to subscribe to - leave empty to subscribe to all events
) (chan Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.workers == nil {
		e.workers = make(map[string]*eventWorker)
	}
	subscriberCt := e.srv.numEventSubscribers.Load()

	e.logger.Debug(
		"subscribe request",
		"subscriber_count", subscriberCt,
		"subscriber_limit", e.subscriberLimit,
	)
	if e.subscriberLimit > 0 && subscriberCt >= e.subscriberLimit {
		e.logger.Warn(
			"subscriber limit reached",
			"subscriber_count", subscriberCt,
			"subscriber_limit", e.subscriberLimit,
		)
		return nil, &KQError{
			Message: fmt.Sprintf(
				"subscriber limit reached: %d",
				e.subscriberLimit,
			),
			Code: codes.ResourceExhausted,
		}
	}

	currentWorker, exists := e.workers[name]
	if exists {
		e.logger.Info(
			"subscriber already exists",
			slog.String("name", name),
			slog.Any("worker", currentWorker),
		)
		return nil, fmt.Errorf("subscriber '%s' already exists", name)
	}

	w := newEventWorker(e, name, events, keys)
	e.workers[name] = w
	e.logger.Log(
		context.Background(),
		LevelNotice,
		"added subscriber",
		"worker",
		w,
	)
	go w.Run(ctx)
	return w.out, nil
}

// Run starts the event stream.
func (e *eventStream) Run(ctx context.Context) {
	e.logger.Log(ctx, LevelNotice, "started event stream")

	// For each event, we start a goroutine that will forward the event to
	// each worker. Once all the workers have handled the event (either by
	// successfully forwarding it, or timing out doing so), we move on to
	// the next event.
	// We could spawn a goroutine for each worker and not wait for them to
	// finish, but if a worker gets backed up, we could end up drowning
	// in goroutines. It also means events may be delivered out of order,
	// which would be fine but is annoying to test.
	for event := range e.events {
		// We don't return when the context is finished - we just move on.
		// The workers will see the context is finished, break out of their
		// loop, and close their outbound channels.
		// This loop will end when Server closes the inbound channel.
		if ctx.Err() != nil {
			continue
		}
		e.logger.Debug("event seen", "event", event)
		e.mu.RLock()
		wg := &sync.WaitGroup{}
		for name, w := range e.workers {
			if ctx.Err() != nil {
				break
			}

			e.logger.Debug(
				"sending to worker",
				"name",
				name,
				"event",
				event,
				"worker",
				w,
			)
			wg.Add(1)
			ww := w
			go func(worker *eventWorker) {
				defer wg.Done()

				worker.mu.RLock()
				defer worker.mu.RUnlock()

				select {
				case <-ctx.Done():
					return
				case worker.in <- event:
					e.logger.Debug(
						"sent to worker",
						"worker",
						worker,
						"event",
						event,
					)
				default:
					// the worker is already buffered, so if the client
					// is slow enough to fill the buffer, we'll just
					// drop the event
				}
			}(ww)
		}
		wg.Wait()
		e.mu.RUnlock()
	}
}

// Stop stops all workers
func (e *eventStream) Stop(ctx context.Context) {
	e.logger.Log(
		ctx,
		LevelNotice,
		"stopping event stream",
		"subscriber_count",
		e.srv.numEventSubscribers.Load(),
	)

	workersStopped := make(chan string, len(e.workers))
	wg := &sync.WaitGroup{}

	for id, w := range e.workers {
		e.logger.Debug("removing worker entry", "worker", w)
		wg.Add(1)
		go func(workerID string, worker *eventWorker) {
			defer wg.Done()

			worker.mu.Lock()
			defer worker.mu.Unlock()

			e.logger.Debug("sending stop signal to worker", "worker", worker)
			worker.done <- struct{}{}
			close(worker.in)
			<-worker.stopped
			workersStopped <- workerID
			e.logger.Info("worker stopped", "worker", worker)
		}(id, w)
	}

	wg.Wait()

	close(workersStopped)
	stopCt := 0
	for workerName := range workersStopped {
		stopCt++
		delete(e.workers, workerName)
	}
	if stopCt > 0 {
		e.logger.Log(ctx, LevelNotice, "workers stopped")
	}
}
