package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func (b BroadcastBody) MarshalJSON() ([]byte, error) {
	m := map[string]string{
		"type": b.Type,
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

func main() {
	messages := make([]int, 0)
	neighbors := make([]string, 0)

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, body.Message)

		// Broadcast message to neighbors.
		for i := range neighbors {
			err := n.Send(neighbors[i], body)
			if err != nil {
				log.Println(err.Error())
			}
		}

		body.Type = "broadcast_ok"

		return n.Reply(msg, body)
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
