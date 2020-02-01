package runner

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
)

// SinkDefinition is the definition of a sink that runs at an interval and will also flush if the buffer is full
type SinkDefinition interface {
	Codec() Codec
	Group() string
	Topic() string
	MaxBufferSize() int
	Interval() time.Duration
	Flush() error
	Collect(ctx MessageContext, key string, msg interface{}) error
}

// MessageContext is the extra kafka context data for the message
type MessageContext struct {
	Partition int32
	Topic     string
	Offset    int64
	Timestamp time.Time
}

// SinkRunner is a sink runner for kafmesh
type SinkRunner struct {
	definition SinkDefinition
	brokers    []string
}

// NewSinkRunner create a new sink runner
func NewSinkRunner(definition SinkDefinition, brokers []string) *SinkRunner {
	return &SinkRunner{
		definition: definition,
		brokers:    brokers,
	}
}

// Run runs the sink
func (r *SinkRunner) Run(ctx context.Context) func() error {
	return func() error {
		config := cluster.NewConfig()
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		config.Version = sarama.V2_0_0_0

		cg, err := cluster.NewConsumer(r.brokers, r.definition.Group(), []string{r.definition.Topic()}, config)
		if err != nil {
			return err
		}
		defer cg.Close()

		err = r.consume(ctx, cg)
		return err
	}
}

func (r *SinkRunner) consume(ctx context.Context, cg *cluster.Consumer) error {
	ticker := time.NewTicker(r.definition.Interval())
	defer ticker.Stop()

	count := 0
	partitionOffsets := make(map[int32]*sarama.ConsumerMessage)

	codec := r.definition.Codec()
	maxBufferSize := r.definition.MaxBufferSize()

	flush := func() error {
		err := r.definition.Flush()
		if err != nil {
			return errors.Wrap(err, "failed flushing")
		}

		for _, offset := range partitionOffsets {
			cg.MarkOffset(offset, "")
		}

		partitionOffsets = make(map[int32]*sarama.ConsumerMessage)
		count = 0

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			err := cg.Close()
			return err

		case <-ticker.C:
			if count > 0 {
				err := flush()
				if err != nil {
					return err
				}
			}
			continue

		case err := <-cg.Errors():
			return errors.Wrap(err, "error received while processing")

		case _ = <-cg.Notifications():
			if count > 0 {
				err := flush()
				if err != nil {
					return err
				}
			}
			continue

		case msg := <-cg.Messages():
			if msg.Topic != r.definition.Topic() {
				continue
			}

			message, err := codec.Decode(msg.Value)
			if err != nil {
				return errors.Wrapf(err, "failed decoding %v", msg.Value)
			}

			msgctx := MessageContext{
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Timestamp: msg.Timestamp,
				Topic:     msg.Topic,
			}

			err = r.definition.Collect(msgctx, string(msg.Key), message)
			if err != nil {
				return errors.Wrapf(err, "failed collecting message")
			}
			count++

			partitionOffsets[msg.Partition] = msg

			if count >= maxBufferSize {
				err := flush()
				if err != nil {
					return err
				}
			}
		}
	}
}
