package bus

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

type Subscriber struct {
	Conn *nats.Conn
}

type Event struct {
	RuleID string `json:"rule_id"`
}

func NewSubscriber(url string) (*Subscriber, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &Subscriber{Conn: conn}, nil
}

func (s *Subscriber) Close() {
	if s.Conn != nil {
		s.Conn.Drain()
		s.Conn.Close()
	}
}

func (s *Subscriber) Subscribe(subject string, handler func(Event)) (*nats.Subscription, error) {
	return s.Conn.Subscribe(subject, func(msg *nats.Msg) {
		var evt Event
		_ = json.Unmarshal(msg.Data, &evt)
		handler(evt)
	})
}
