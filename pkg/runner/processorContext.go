package runner

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/observability"
	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"

	"github.com/golang/protobuf/ptypes"
)

// ProcessorContext is a context for processor observability
type ProcessorContext struct {
	context.Context
	component string
	processor string
	key       string
	watcher   *observability.Watcher
	operation *watchv1.Operation
}

// Input registers an input
func (c *ProcessorContext) Input(topic, message, value string) {
	if c.operation == nil {
		return
	}

	c.operation.StartTime = ptypes.TimestampNow()
	c.operation.Input = &watchv1.Input{
		Topic:   topic,
		Message: message,
		Value:   value,
	}
}

// Join registers a join
func (c *ProcessorContext) Join(topic, message, value string) {
	if c.operation == nil {
		return
	}

	c.operation.Actions = append(c.operation.Actions, &watchv1.Action{
		Action: &watchv1.Action_ActionJoin{
			ActionJoin: &watchv1.Join{
				Topic:   topic,
				Message: message,
				Value:   value,
			},
		},
	})
}

// Lookup registers a lookup
func (c *ProcessorContext) Lookup(topic, message, key, value string) {
	if c.operation == nil {
		return
	}

	c.operation.Actions = append(c.operation.Actions, &watchv1.Action{
		Action: &watchv1.Action_ActionLookup{
			ActionLookup: &watchv1.Lookup{
				Topic:   topic,
				Message: message,
				Key:     key,
				Value:   value,
			},
		},
	})
}

// GetState registers a persistence get state
func (c *ProcessorContext) GetState(topic, message, value string) {
	if c.operation == nil {
		return
	}

	c.operation.Actions = append(c.operation.Actions, &watchv1.Action{
		Action: &watchv1.Action_ActionGetState{
			ActionGetState: &watchv1.GetState{
				Topic:   topic,
				Message: message,
				Value:   value,
			},
		},
	})
}

// SetState registers a persistence set state
func (c *ProcessorContext) SetState(topic, message, value string) {
	if c.operation == nil {
		return
	}

	c.operation.Actions = append(c.operation.Actions, &watchv1.Action{
		Action: &watchv1.Action_ActionSetState{
			ActionSetState: &watchv1.SetState{
				Topic:   topic,
				Message: message,
				Value:   value,
			},
		},
	})
}

// Output registers an output
func (c *ProcessorContext) Output(topic, message, key, value string) {
	if c.operation == nil {
		return
	}

	c.operation.Actions = append(c.operation.Actions, &watchv1.Action{
		Action: &watchv1.Action_ActionOutput{
			ActionOutput: &watchv1.Output{
				Topic:   topic,
				Message: message,
				Key:     key,
				Value:   value,
			},
		},
	})
}

// Finish sends the operation to observers
func (c *ProcessorContext) Finish() {
	if c.operation == nil {
		return
	}

	c.operation.EndTime = ptypes.TimestampNow()
	c.watcher.Send(c.component, c.processor, c.key, c.operation)
}
