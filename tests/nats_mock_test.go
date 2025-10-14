package tests

import (
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// MockJS is a lightweight in-memory implementation of message.JSContext
// suitable for unit tests without a running NATS server.
type MockJS struct {
	mu          sync.Mutex
	subscribers map[string][]*mockSubscriber
	queueSubs   map[string][]*mockSubscriber
	queueIndex  map[string]int
	allMessages []*nats.Msg
	streams     map[string]*nats.StreamInfo
	consumers   map[string]map[string]*nats.ConsumerInfo // stream -> consumer -> info
}

type mockSubscriber struct {
	subject string
	queue   string
	cb      nats.MsgHandler
	active  bool
}

func NewMockJS() *MockJS {
	return &MockJS{
		subscribers: make(map[string][]*mockSubscriber),
		queueSubs:   make(map[string][]*mockSubscriber),
		queueIndex:  make(map[string]int),
		streams:     make(map[string]*nats.StreamInfo),
		consumers:   make(map[string]map[string]*nats.ConsumerInfo),
	}
}

// SupportsAcks indicates ack/nak are not supported in the mock; the SDK will no-op.
func (m *MockJS) SupportsAcks() bool { return false }

func (m *MockJS) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	m.mu.Lock()
	// Keep a copy for pull-based fetches
	msg := &nats.Msg{Subject: subj, Data: data}
	m.allMessages = append(m.allMessages, msg)

	// Deliver to push subscribers
	if subs := m.subscribers[subj]; len(subs) > 0 {
		// invoke outside lock to avoid deadlocks
		callbacks := make([]nats.MsgHandler, 0, len(subs))
		for _, s := range subs {
			if s.active {
				callbacks = append(callbacks, s.cb)
			}
		}
		m.mu.Unlock()
		for _, cb := range callbacks {
			cb(&nats.Msg{Subject: subj, Data: data})
		}
		m.mu.Lock()
	}

	// Deliver to one of the queue subscribers (round-robin)
	if subs := m.queueSubs[subj]; len(subs) > 0 {
		idx := m.queueIndex[subj] % len(subs)
		sel := subs[idx]
		m.queueIndex[subj] = (idx + 1) % len(subs)
		if sel.active {
			// Ensure queue field is properly set for queue subscribers
			_ = sel.queue // mark field as used
			cb := sel.cb
			m.mu.Unlock()
			cb(&nats.Msg{Subject: subj, Data: data})
			m.mu.Lock()
		}
	}

	m.mu.Unlock()
	return &nats.PubAck{Stream: "MOCK", Sequence: uint64(len(m.allMessages))}, nil
}

func (m *MockJS) Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (message.JSSubscription, error) {
	m.mu.Lock()
	sub := &mockSubscriber{subject: subj, cb: cb, active: true, queue: ""} // empty queue for regular subscribers
	m.subscribers[subj] = append(m.subscribers[subj], sub)
	m.mu.Unlock()
	return &mockSubscription{owner: m, subscriber: sub}, nil
}

func (m *MockJS) PullSubscribe(subj, durable string, opts ...nats.SubOpt) (message.JSSubscription, error) {
	m.mu.Lock()
	sub := &mockSubscriber{subject: subj, cb: nil, active: true, queue: durable}
	m.queueSubs[subj] = append(m.queueSubs[subj], sub)
	m.mu.Unlock()
	// For simplicity, return a subscription that fetches from the shared buffer
	return &mockPullSubscription{owner: m, durable: durable}, nil
}

func (m *MockJS) StreamInfo(stream string) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info, exists := m.streams[stream]; exists {
		return info, nil
	}
	return nil, nats.ErrStreamNotFound
}

func (m *MockJS) AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	info := &nats.StreamInfo{
		Config: *cfg,
		State: nats.StreamState{
			Msgs:      0,
			Bytes:     0,
			FirstSeq:  1,
			LastSeq:   0,
			Consumers: 0,
		},
	}
	m.streams[cfg.Name] = info
	return info, nil
}

func (m *MockJS) ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if streamConsumers, exists := m.consumers[stream]; exists {
		if info, exists := streamConsumers[consumer]; exists {
			return info, nil
		}
	}
	return nil, nats.ErrConsumerNotFound
}

func (m *MockJS) AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.consumers[stream] == nil {
		m.consumers[stream] = make(map[string]*nats.ConsumerInfo)
	}
	info := &nats.ConsumerInfo{
		Stream: stream,
		Name:   cfg.Durable,
		Config: *cfg,
	}
	m.consumers[stream][cfg.Durable] = info
	return info, nil
}

type mockSubscription struct {
	owner      *MockJS
	subscriber *mockSubscriber
	drained    bool
}

func (s *mockSubscription) Unsubscribe() error {
	s.owner.mu.Lock()
	s.subscriber.active = false
	s.owner.mu.Unlock()
	return nil
}

func (s *mockSubscription) Drain() error {
	s.drained = true
	// simulate drain wait
	time.Sleep(1 * time.Millisecond)
	return s.Unsubscribe()
}

func (s *mockSubscription) IsValid() bool { return s.subscriber.active }

func (s *mockSubscription) Pending() (int, int, error) { return 0, 0, nil }

func (s *mockSubscription) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return nil, nil
}

type mockPullSubscription struct {
	owner   *MockJS
	durable string
}

func (s *mockPullSubscription) Unsubscribe() error         { return nil }
func (s *mockPullSubscription) Drain() error               { return nil }
func (s *mockPullSubscription) IsValid() bool              { return true }
func (s *mockPullSubscription) Pending() (int, int, error) { return 0, 0, nil }

func (s *mockPullSubscription) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if batch <= 0 {
		batch = 10
	}
	n := batch
	if n > len(s.owner.allMessages) {
		n = len(s.owner.allMessages)
	}
	msgs := make([]*nats.Msg, n)
	copy(msgs, s.owner.allMessages[:n])
	// pop from buffer
	s.owner.allMessages = s.owner.allMessages[n:]
	return msgs, nil
}
