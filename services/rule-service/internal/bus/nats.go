package bus

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

type Publisher struct {
	Conn *nats.Conn
}

func NewPublisher(url string) (*Publisher, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &Publisher{Conn: conn}, nil
}

func (p *Publisher) Close() {
	if p.Conn != nil {
		p.Conn.Drain()
		p.Conn.Close()
	}
}

func (p *Publisher) Publish(subject string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return p.Conn.Publish(subject, data)
}
