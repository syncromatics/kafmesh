package generator_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/syncromatics/kafmesh/internal/generator"
	"github.com/syncromatics/kafmesh/internal/models"
)

func Test_Generator(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "Test_Generator")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(tmpDir)

	ioutil.WriteFile(path.Join(tmpDir, "go.mod"), []byte(`module test

go 1.16`), os.ModePerm)

	protoDir := path.Join(tmpDir, "protos")
	err = os.MkdirAll(protoDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	package1 := path.Join(protoDir, "testMesh", "testId")
	err = os.MkdirAll(package1, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(package1, "test.proto"), []byte(`syntax ="proto3";
package testMesh.testId;

message Test {
	string name = 1;
}`), os.ModePerm)

	ioutil.WriteFile(path.Join(package1, "test2.proto"), []byte(`syntax ="proto3";
package testMesh.testId;

import "google/protobuf/timestamp.proto";

message Test2 {
	string serial = 1;
	google.protobuf.Timestamp time = 2;
}`), os.ModePerm)

	package2 := path.Join(protoDir, "testMesh", "testSerial")
	err = os.MkdirAll(package2, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(package2, "details.proto"), []byte(`syntax ="proto3";
package testMesh.testSerial;

message Details {
	string name = 1;
}`), os.ModePerm)

	ioutil.WriteFile(path.Join(package2, "detailsState.proto"), []byte(`syntax ="proto3";
package testMesh.testSerial;

message DetailsState {
	string name = 1;
}`), os.ModePerm)

	ioutil.WriteFile(path.Join(package2, "detailsEnriched.proto"), []byte(`syntax ="proto3";
package testMesh.testSerial;

message DetailsEnriched {
	string name = 1;
}`), os.ModePerm)

	rawType := "raw"
	externalTopicName := "externalTopic"
	newPath := path.Join(tmpDir, "defin")
	options := generator.Options{
		Service: &models.Service{
			Name: "testMesh",
			Output: models.OutputSettings{
				Path:    "internal/kafmesh",
				Package: "kafmesh",
				Module:  "test",
			},
			Messages: models.MessageDefinitions{
				Protobuf: []string{
					"../protos",
				},
			},
			Defaults: models.TopicDefaults{
				Partition:   10,
				Replication: 1,
				Retention:   24 * time.Hour,
				Segment:     12 * time.Hour,
				Type:        "protobuf",
			},
		},
		RootPath:        newPath,
		DefinitionsPath: newPath,
		Components: []*models.Component{
			{
				Name: "details",
				Processors: []models.Processor{
					{
						Name: "enricher",
						Inputs: []models.Input{
							{
								TopicDefinition: models.TopicDefinition{
									Message: "testId.test",
								},
							},
							{
								TopicDefinition: models.TopicDefinition{
									Message: "testId.test2",
								},
							},
						},
						Lookups: []models.Lookup{
							{
								TopicDefinition: models.TopicDefinition{
									Message: "testSerial.details",
								},
							},
						},
						Joins: []models.Join{
							{
								TopicDefinition: models.TopicDefinition{
									Message: "testSerial.details",
								},
							},
						},
						Outputs: []models.Output{
							{
								TopicDefinition: models.TopicDefinition{
									Message: "testSerial.detailsEnriched",
								},
							},
						},
						Persistence: &models.Persistence{
							TopicDefinition: models.TopicDefinition{
								Message: "testSerial.detailsState",
							},
						},
					},
					{
						Name: "enricher",
						Inputs: []models.Input{
							{
								TopicDefinition: models.TopicDefinition{
									Message: "someMessage",
									Type:    &rawType,
									Topic:   &externalTopicName,
								},
							},
							{
								TopicDefinition: models.TopicDefinition{
									Message: "testId.test2",
								},
							},
						},
						Outputs: []models.Output{
							{
								TopicDefinition: models.TopicDefinition{
									Message: "testSerial.detailsEnriched",
								},
							},
						},
					},
				},
				Sources: []models.Source{
					{
						TopicDefinition: models.TopicDefinition{
							Message: "testSerial.details",
						},
					},
				},
				Sinks: []models.Sink{
					{
						Name: "Enriched Data Postgres",
						TopicDefinition: models.TopicDefinition{
							Message: "testSerial.detailsEnriched",
						},
					},
				},
				Views: []models.View{
					{
						TopicDefinition: models.TopicDefinition{
							Message: "testSerial.detailsEnriched",
						},
					},
				},
				ViewSources: []models.ViewSource{
					{
						Name: "test to database",
						TopicDefinition: models.TopicDefinition{
							Message: "testId.test",
						},
					},
				},
				ViewSinks: []models.ViewSink{
					{
						Name: "test to api",
						TopicDefinition: models.TopicDefinition{
							Message: "testId.test",
						},
					},
				},
			},
		},
	}

	err = generator.Generate(options)
	if err != nil {
		t.Fatal(err)
	}

	validateProcessors(newPath, t)
	validateEmitter(newPath, t)
	validateSink(newPath, t)
	validateView(newPath, t)
	validateViewSource(newPath, t)
	validateViewSink(newPath, t)
	validateService(newPath, t)
	validateTopic(newPath, t)
}
