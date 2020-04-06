package services

import (
	"context"
	"fmt"
	discoverv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discover/v1"
	"github.com/syncromatics/kafmesh/pkg/models"
)

// DiscoverAPI provides methods for discovering and reporting kafmesh enabled pod info
type DiscoverAPI struct {
	DiscoverInfo *models.DiscoverInfo
}

// GetServiceInfo is a RPC method that returns service info for discovery
func (s *DiscoverAPI) GetServiceInfo(ctx context.Context, request *discoverv1.DiscoverRequest) (*discoverv1.DiscoverResponse, error) {

	str := fmt.Sprintf("%+v", s.DiscoverInfo)

	var component = discoverv1.Component{
		Name:        s.DiscoverInfo.Component.Name,
		Description: s.DiscoverInfo.Component.Description,
	}

	//Convert processors
	for _, p := range s.DiscoverInfo.Component.Processors {
		processor := &discoverv1.Processor{
			Name: p.Name,
			//GroupNameOverride: p.GroupNameOverride,
			Description: p.Description,
			Inputs:      nil,
			Lookups:     nil,
			Joins:       nil,
			Outputs:     nil,
			Persistence: nil,
		}

		//Convert processor inputs
		for _, i := range p.Inputs {
			input := &discoverv1.Input{
				TopicDefinition: &discoverv1.TopicDefinition{
					Message: i.Message,
					//Type:    *i.Type,
					//Topic:   *i.Topic,
				},
			}
			processor.Inputs = append(processor.Inputs, input)
		}
		
		//Convert processor lookups
		for _, i := range p.Lookups {
			lookup := &discoverv1.Lookup{
				TopicDefinition: &discoverv1.TopicDefinition{
					Message: i.Message,
					//Type:    *i.Type,
					//Topic:   *i.Topic,
				},
			}
			processor.Lookups = append(processor.Lookups, lookup)
		}

		//Convert processor joins
		for _, i := range p.Joins {
			join := &discoverv1.Join{
				TopicDefinition: &discoverv1.TopicDefinition{
					Message: i.Message,
					//Type:    *i.Type,
					//Topic:   *i.Topic,
				},
			}
			processor.Joins = append(processor.Joins, join)
		}

		//Convert processor outputs
		for _, i := range p.Outputs {
			output := &discoverv1.Output{
				TopicDefinition: &discoverv1.TopicDefinition{
					Message: i.Message,
					//Type:    *i.Type,
					//Topic:   *i.Topic,
				},
			}
			processor.Outputs = append(processor.Outputs, output)
		}

		//Convert persistence
		persistence := &discoverv1.Persistence{
			TopicDefinition: &discoverv1.TopicDefinition{
				Message: s.DiscoverInfo.Component.Persistence.Message,
				//Type:    *i.Type,
				//Topic:   *i.Topic,
			},
		}
		component.Persistence = persistence
		
		component.Processors = append(component.Processors, processor)
	}
	//done adding processors

	return &discoverv1.DiscoverResponse{
		GoStruct:           str,
		ServiceName:        s.DiscoverInfo.ServiceName,
		ServiceDescription: s.DiscoverInfo.ServiceDescription,
		Component:          &component,
	}, nil
}
