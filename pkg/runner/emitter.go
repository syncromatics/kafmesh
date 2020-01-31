package runner

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

// EmitMessage is the message to be emitted
type EmitMessage interface {
	Key() string
	Value() interface{}
}

// Emitter is the emitter for a goka stream
type Emitter struct {
	emitter *goka.Emitter
	sem     *semaphore.Weighted

	criticalFailure chan error
}

// NewEmitter creates a new wrapped goka emitter
func NewEmitter(emitter *goka.Emitter) *Emitter {
	return &Emitter{
		emitter:         emitter,
		sem:             semaphore.NewWeighted(1000),
		criticalFailure: make(chan error),
	}
}

// Emit emits a message and waits for the ack
func (e *Emitter) Emit(key string, msg interface{}) error {
	err := e.emitter.EmitSync(key, msg)
	if err != nil {
		var critical error
		switch err {
		case sarama.ErrOutOfBrokers:
			critical = errors.Wrap(err, "fatal emitter error")
		case sarama.ErrClosedClient:
			critical = errors.Wrap(err, "fatal emitter error")
		case sarama.ErrNotConnected:
			critical = errors.Wrap(err, "fatal emitter error")
		}

		if critical == nil {
			return errors.Wrap(err, "failed emitting message")
		}

		select {
		case e.criticalFailure <- critical:
		default:
			fmt.Println("warning: critical failure in emitter with no watch")
		}
	}
	return nil
}

// EmitBulk emits messages and waits for them all to complete
func (e *Emitter) EmitBulk(ctx context.Context, msgs []EmitMessage) error {
	count := len(msgs)
	promises := make(chan error, count)
	done := make(chan error, 1)

	go func() {
		for _, msg := range msgs {
			err := e.sem.Acquire(ctx, 1)
			if err != nil {
				done <- errors.Wrap(err, "failed acquiring semaphore")
				break
			}

			p, err := e.emitter.Emit(msg.Key(), msg.Value())
			if err != nil {
				e.sem.Release(1)
				done <- errors.Wrap(err, "failed emitting message")
				break
			}

			p.Then(func(asyncErr error) {
				e.sem.Release(1)
				promises <- asyncErr
			})
		}
	}()

	var lastError error
	for i := 0; i < count; i++ {
		select {
		case err := <-promises:
			if err != nil {
				var critical error
				switch err {
				case sarama.ErrOutOfBrokers:
					critical = errors.Wrap(err, "fatal emitter error")
				case sarama.ErrClosedClient:
					critical = errors.Wrap(err, "fatal emitter error")
				case sarama.ErrNotConnected:
					critical = errors.Wrap(err, "fatal emitter error")
				}

				lastError = err

				if critical == nil {
					continue
				}

				select {
				case e.criticalFailure <- critical:
				default:
					fmt.Println("warning: critical failure in emitter with no watch")
				}
			}
		case err := <-done:
			lastError = err
			break
		case <-ctx.Done():
			lastError = fmt.Errorf("context canceled")
			break
		}
	}

	if lastError != nil {
		return errors.Wrapf(lastError, "failed sending bulk message with %d messages", len(msgs))
	}

	return nil
}

// Delete produces a nil for the key to kafka
func (e *Emitter) Delete(key string) error {
	err := e.emitter.EmitSync(key, nil)
	if err != nil {
		var critical error
		switch err {
		case sarama.ErrOutOfBrokers:
			critical = errors.Wrap(err, "fatal emitter error")
		case sarama.ErrClosedClient:
			critical = errors.Wrap(err, "fatal emitter error")
		case sarama.ErrNotConnected:
			critical = errors.Wrap(err, "fatal emitter error")
		}

		if critical == nil {
			return errors.Wrap(err, "failed emitting message")
		}

		select {
		case e.criticalFailure <- critical:
		default:
			fmt.Println("warning: critical failure in emitter with no watch")
		}

		return errors.Wrap(err, "failed emitting message")
	}
	return nil
}

// Watch watches for critial errors on the emitter
func (e *Emitter) Watch(ctx context.Context) func() error {
	return func() error {
		select {
		case <-ctx.Done():
			return e.emitter.Finish()
		case m := <-e.criticalFailure:
			return m
		}
	}
}
