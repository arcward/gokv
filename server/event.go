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
	NoEvent     KeyEvent = iota
	Created              // New key has been created
	Updated              // Key value has been updated
	Deleted              // Key has been deleted
	Expired              // Key has expired
	Locked               // Key has been locked, or re-locked
	Unlocked             // Key has been unlocked
	Expunged             // Key has pruned
	Accessed             // Key value has been accessed via Get or Inspect
	LifespanSet          // When Lifespan is either set, updated or renewed
)

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
	case NoEvent:
		return "NO_EVENT"
	default:
		panic(fmt.Sprintf("unknown event: %d", ke))
	}
}

func (ke KeyEvent) LogValue() slog.Value {
	return slog.StringValue(ke.String())
}

type Event struct {
	Key      string    `json:"key"`
	Event    KeyEvent  `json:"event"`
	Time     time.Time `json:"time"`
	ClientID string    `json:"client_id"`
}

func (e Event) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", e.Key),
		slog.String("event", e.Event.String()),
		slog.Time("time", e.Time),
		slog.String("client_id", e.ClientID),
	)
}

type EventWorker struct {
	// random id to differentiate workers of the same name over time
	id   string
	name string
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
	logger      *slog.Logger
	// published is the number of events published by this worker
	published atomic.Uint64
	// timeouts is the number of events that timed out when sending
	timeouts      atomic.Uint64
	running       bool
	mu            sync.RWMutex
	once          sync.Once
	includeEvents []KeyEvent
	includeKeys   []string
	es            *eventStream
}

func NewEventWorker(
	es *eventStream,
	name string,
	includeEvents []KeyEvent,
	includeKeys []string,
) *EventWorker {
	id := make([]byte, 12/2)
	if _, idErr := rand.Read(id); idErr != nil {
		panic(fmt.Sprintf("error creating worker ID: %s", idErr.Error()))
	}
	workerID := hex.EncodeToString(id)
	w := &EventWorker{
		id:   workerID,
		name: name,
		out:  make(chan Event, DefaultEventStreamBufferSize),
		in:   make(chan Event, DefaultEventStreamBufferSize),
		// sendTimeout:   e.SendTimeout,
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
		),
	)
	return w
}

func (w *EventWorker) LogValue() slog.Value {
	return slog.GroupValue(
		// slog.String("name", w.name),
		slog.String("id", w.id),
		slog.String("name", w.name),
	)
}

func (w *EventWorker) Run(ctx context.Context) {
	w.run(ctx)
}

func (w *EventWorker) run(ctx context.Context) {
	sendTimeout := w.es.SendTimeout
	w.logger.Info("starting subscriber worker", "send_timeout", sendTimeout)
	w.mu.Lock()
	if w.running {
		panic(fmt.Sprintf("worker already running %s", w.name))
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w.running = true
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
		case event, ok := <-w.in:
			if !ok {
				break EventLoop
			}
			if w.includeEvents != nil && !sliceContains(
				w.includeEvents,
				event.Event,
			) {
				continue EventLoop
			}
			if w.includeKeys != nil && !sliceContains(
				w.includeKeys, event.Key,
			) {
				continue
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
			// w.mu.Lock()
			t := time.NewTimer(sendTimeout)
			select {
			case w.out <- event:
				// sent
				w.logger.Debug("worker sent event", "event", event)
				w.published.Add(1)
			case <-t.C:
				// timeout
				w.timeouts.Add(1)
				w.logger.Warn("event send timeout", "event", event)
			}
			eventSpan.End()
		}
	}

	close(w.out)
	w.stopped <- struct{}{}
	close(w.stopped)
	w.es.srv.numEventSubscribers.Add(^uint64(0))
	w.logger.Info(
		"subscriber metrics",
		"published", w.published.Load(),
		"timeouts", w.timeouts.Load(),
	)
}

type eventStream struct {
	// SendTimeout is the amount of time to wait for an event to be sent from
	// a worker to a subscriber, to avoid blocking.
	SendTimeout     time.Duration
	SubscriberLimit uint64
	// events is a channel receiving events from the main event channel
	events chan Event
	// workers maps the name provided in Subscribe, to a worker goroutine
	// responsible for forwarding events to the subscriber
	workers map[string]*EventWorker
	mu      sync.RWMutex
	logger  *slog.Logger
	running bool
	srv     *KeyValueStore
}

func NewEventStream(srv *KeyValueStore) *eventStream {
	ev := &eventStream{
		SendTimeout:     srv.cfg.EventStreamSendTimeout,
		SubscriberLimit: srv.cfg.EventStreamSubscriberLimit,
		logger:          srv.logger.With(loggerKey, "event_stream"),
		srv:             srv,
	}
	return ev
}

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
	close(w.in)

	e.logger.Info(
		"removed subscriber",
		"subscriber",
		name,
		"subscriber_count",
		e.srv.numEventSubscribers.Load(),
		"worker",
		w,
	)
	e.logger.Debug("waiting for stop signal", "worker", w)
	<-w.stopped
	w.mu.Unlock()
	e.logger.Debug("got stop signal", "worker", w)
	return nil
}

func (e *eventStream) Publish(event Event) {
	e.mu.RLock()
	running := e.running
	e.mu.RUnlock()
	if running {
		e.events <- event
	}
}

func (e *eventStream) Subscribe(
	ctx context.Context,
	name string,
	keys []string,
	events []KeyEvent,
) (chan Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.workers == nil {
		e.workers = make(map[string]*EventWorker)
	}
	subscriberCt := e.srv.numEventSubscribers.Load()

	e.logger.Debug(
		"subscribe request",
		"subscriber_count", subscriberCt,
		"subscriber_limit", e.SubscriberLimit,
	)
	if e.SubscriberLimit > 0 && subscriberCt >= e.SubscriberLimit {
		e.logger.Warn(
			"subscriber limit reached",
			"subscriber_count", subscriberCt,
			"subscriber_limit", e.SubscriberLimit,
		)
		return nil, &GOKVError{
			Message: fmt.Sprintf(
				"subscriber limit reached: %d",
				e.SubscriberLimit,
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

	w := NewEventWorker(e, name, events, keys)

	e.logger.Info(
		"added subscriber to event stream",
		"worker",
		w,
	)
	e.workers[name] = w
	go w.Run(ctx)
	return w.out, nil
}

func (e *eventStream) Stop() {
	e.logger.Info(
		"stopping event stream",
		"subscriber_count",
		e.srv.numEventSubscribers.Load(),
	)
	e.running = false

	workersStopped := []string{}
	for workerID, w := range e.workers {
		e.logger.Debug("removing worker entry", "worker", w)
		w.mu.Lock()
		delete(e.workers, workerID)
		e.logger.Debug("sending stop signal to worker", "worker", w)
		w.done <- struct{}{}
		close(w.in)
		<-w.stopped
		e.logger.Info("worker stopped", "worker", w)
		workersStopped = append(workersStopped, workerID)
		w.mu.Unlock()
	}
}

func (e *eventStream) Run(ctx context.Context) {
	e.logger.Info("starting event stream")
	e.mu.Lock()
	if e.running {
		e.logger.Error("already running")
		panic("event stream already running")
	}
	e.running = true
	e.mu.Unlock()

	for event := range e.events {
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
			go func(worker *EventWorker) {
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
				}
			}(ww)
		}
		wg.Wait()
		e.mu.RUnlock()
	}
}
