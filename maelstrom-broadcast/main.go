package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastBody struct {
	Type      string `json:"type"`
	Message   int    `json:"message"`
	MessageId *int    `json:"msg_id,omitempty"`
}

func (b BroadcastBody) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"type": b.Type,
	}

	if b.Type == "broadcast" {
		m["message"] = b.Message
	}

	return json.Marshal(m)
}

type ReadBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type TopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func (t TopologyBody) MarshalJSON() ([]byte, error) {
	m := map[string]string{
		"type": t.Type,
	}

	return json.Marshal(m)
}

func Contains[T comparable](values []T, v T) bool {
	for i := range values {
		if values[i] == v {
			return true
		}
	}
	return false
}

func main() {
	messages := make([]int, 0)
	neighbors := make([]string, 0)

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if !Contains(messages, body.Message) {
			messages = append(messages, body.Message)

			// Broadcast message to neighbors.
			for i := range neighbors {
				err := n.Send(neighbors[i], body)
				if err != nil {
					log.Println(err.Error())
				}
			}
		}

		if body.MessageId != nil {
			body.Type = "broadcast_ok"
			return n.Reply(msg, body)
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body ReadBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body.Type = "read_ok"
		body.Messages = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := n.ID()
		neighbors = body.Topology[id]

		body.Type = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
