package generator_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/syncromatics/kafmesh/pkg/generator"
	"github.com/syncromatics/kafmesh/pkg/models"
)

func Test_Generator(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "Test_Generator")
	if err != nil {
		t.Fatal(err)
	}
	tmpDir = path.Join("/tmp", "genTest")

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

	options := generator.Options{
		Service: &models.Service{
			Name: "testService",
			Output: models.OutputSettings{
				Path:    "internal/kafmesh",
				Package: "kafmesh",
				Module:  "test",
			},
			Messages: models.MessageDefinitions{
				Protobuf: []string{
					"./protos",
				},
			},
			Defaults: models.TopicDefaults{
				Partition:   10,
				Replication: 1,
				Retention:   24 * time.Hour,
				Segment:     12 * time.Hour,
			},
		},
		RootPath:        tmpDir,
		DefinitionsPath: tmpDir,
		Components: []*models.Component{
			&models.Component{
				Name: "details",
				Processors: []models.Processor{
					models.Processor{
						Name: "enricher",
						Inputs: []models.Input{
							models.Input{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testId.test",
								},
							},
							models.Input{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testId.test2",
								},
							},
						},
						Lookups: []models.Lookup{
							models.Lookup{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testSerial.details",
								},
							},
						},
						Joins: []models.Join{
							models.Join{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testSerial.details",
								},
							},
						},
						Outputs: []models.Output{
							models.Output{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testSerial.detailsEnriched",
								},
							},
						},
						Persistence: &models.Persistence{
							TopicDefinition: models.TopicDefinition{
								Message: "testMesh.testSerial.detailsState",
							},
						},
					},
				},
				Emitters: []models.Emitter{
					models.Emitter{
						TopicDefinition: models.TopicDefinition{
							Message: "testMesh.testSerial.details",
						},
					},
				},
				Sinks: []models.Sink{
					models.Sink{
						Name: "Enriched Data Postgres",
						TopicDefinition: models.TopicDefinition{
							Message: "testMesh.testSerial.detailsEnriched",
						},
					},
				},
				Views: []models.View{
					models.View{
						TopicDefinition: models.TopicDefinition{
							Message: "testMesh.testSerial.detailsEnriched",
						},
					},
				},
				Synchronizers: []models.Synchronizer{
					models.Synchronizer{
						Name: "test to database",
						TopicDefinition: models.TopicDefinition{
							Message: "testMesh.testId.test",
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

	validateProcessors(tmpDir, t)
	validateEmitter(tmpDir, t)
	validateSink(tmpDir, t)
	validateView(tmpDir, t)
	validateSynchronizer(tmpDir, t)
	validateService(tmpDir, t)
}
