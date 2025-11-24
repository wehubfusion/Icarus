package tests

import "github.com/wehubfusion/Icarus/pkg/message"

func buildConsumerGraphForMessage(msg *message.Message) map[string][]string {
	graph := make(map[string][]string)
	parentID := ""
	if msg.Node != nil {
		parentID = msg.Node.NodeID
		graph[parentID] = []string{}
	}

	appendUnique := func(source, consumer string) {
		if source == "" {
			return
		}
		list := graph[source]
		for _, existing := range list {
			if existing == consumer {
				return
			}
		}
		graph[source] = append(list, consumer)
	}

	for _, node := range msg.EmbeddedNodes {
		if _, ok := graph[node.NodeID]; !ok {
			graph[node.NodeID] = []string{}
		}
		for _, mapping := range node.FieldMappings {
			source := mapping.SourceNodeID
			if source == "" {
				source = parentID
			}
			appendUnique(source, node.NodeID)
		}
	}

	return graph
}
