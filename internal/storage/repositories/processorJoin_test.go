package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_ProcessorJoin_Processor(t *testing.T) {
	repo := repos.ProcessorJoin()

	r, err := repo.ProcessorByJoins(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Processor{
		&model.Processor{ID: 1, Name: "processor1", Description: "processor1 description"},
		&model.Processor{ID: 1, Name: "processor1", Description: "processor1 description"},
		&model.Processor{ID: 2, Name: "processor2", Description: "processor2 description"},
		&model.Processor{ID: 2, Name: "processor2", Description: "processor2 description"},
	})
}

func Test_ProcessorJoin_Topic(t *testing.T) {
	repo := repos.ProcessorJoin()

	r, err := repo.TopicByJoins(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Topic{
		&model.Topic{ID: 1, Name: "topic1", Message: "topic1.message"},
		&model.Topic{ID: 2, Name: "topic2", Message: "topic2.message"},
		&model.Topic{ID: 1, Name: "topic1", Message: "topic1.message"},
		&model.Topic{ID: 2, Name: "topic2", Message: "topic2.message"},
	})
}
