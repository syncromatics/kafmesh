# Kafmesh

Kafmesh is...

## Components

### Kafmesh-Discover

Service running in K8s that scrapes all pods in the cluster to discover kafmesh-enabled services. It then reports relevant information to Neo4J where it can be used in GUI.

##### What does kafmesh-discover do exactly?
1. Search all pods in cluster for annotation `kafmesh/scrape: "true"` every 1 minute.
2. Once found, makes GRPC call to pod's parent-service for relevant information like...
    - Service Name
    - Service Description
	- Components
		- Name
		- Description
		- Sources
		- Processors
		- Sinks
		- Views
		- ViewSources
		- ViewSinks
    - Pod Names

It also contains the inputs, joins and lookups relevant to the Components as well as their relevant Kafka topic names
 (aka: messages)
3. It then sends the found information to Neo4j.

##### How Do Kafmesh-enabled services generate the discovery information?
1. Each Kafmesh enabled service uses Kafmesh CLI to generate code at build-time. This creates .yaml used by Kafmesh
 (found in, for example, `enplug-service/docs/kafmesh/components/balena.yaml`)
2. As the generator is taking in .yaml and generating code for registering things with kafka, it is also appending
 that .yaml information to the Service.discoverInfo slice.

##### How does the kafmesh-discovery service retrieve that information?
1. Kafmesh-enabled services, somewhere in their code, have to import the `kafmesh/runner` package and register the
 service. Internally this invokes the code found at `kafmesh/pkg/runner/NewService`.
2. During service registration, Ping and Discover API Servers are registered (Ex: `pingv1.RegisterPingAPIServer`)
3. This API knows to expect the Discover .yaml to present in the enplug-service among the rest of the generated code.

```json
{
  "service": {
    "name": "enplugService",
    "description": "Enplug service manages infotainment enplug systems.",
    "components": [
      {
        "name": "balena",
        "description": "handles synchronizing balena with the mesh",
        "sources": [],
        "processors": [
          {
            "name": "device",
            "description": "handles device serial to balena id mappings",
            "inputs": [
              {
                "message": "deviceSerial.details"
              }
            ],
            "joins": [
              {
                "message": "deviceSerial.assignedVehicle"
              }
            ],
            "lookups": [
              {
                "message": "vehiclesService.vehicleId.customer"
              },
              {
                "message": "vehiclesService.vehicleId.details"
              },
              {
                "message": "vehiclesService.customerId.details"
              }
            ],
            "outputs": {
              "message": "balenaId.device",
              "compact": true
            }
          }
        ],
        "sinks": [],
        "views": [],
        "viewSources": [],
        "viewSinks": [
          {
            "name": "device",
            "message": "balenaId.device"
          }
        ]
      }
    ],
    "messages": [
      {//also messages ^^^ here?}
    ]
  }
}
```
Other components not shown here (from enplug example):
 - details
 - enplug
 - heartbeats
 - vehicles

##### Notes (ToDo: Delete)
- Generally, for kafmesh enabled services, business-logic RPC calls are found in the target service's repo (Example: `enplug-service/docs/protos/syncromatics/enplug/assignment/v1/assignment_api.proto`)
- GRPCurl Example: `grpcurl -plaintext enplug-service:443 kafmesh.ping.v1.PingAPI/Ping` 
`grpcurl -plaintext enplug-service:443 kafmesh.discover.v1.Discover/GetServiceInfo`

This file is generated when kafmesh-gen is run, prior to building your project.
`internal/definitions/service.km.go`
It makes a bunch of GRPC methods available to the outside once the service is up and running, based on your docs/.yaml



---



You install kafmesh (CLI tool) on your machine.
You create an enplug/docs/kafmesh/components directory and a enplug/docs/kafmesh/definition.yml
	The definition.yml file describes the service you are creating (that uses Kafka)
	The 'components' describe the different types of Kafka producers and consumers you want to create, and their topic names.

You run `kafmesh-gen` docs/kafmesh/definition.yml
This runs the code found in the kafmesh repo under kafmesh/cmd/kafmesh-gen/cmd/root.go
	It takes in the .yaml defining the Kafka consumers/producers you want to create, starting with the `definition.yml` and turns it into a Golang struct of type "*Service"
	
	type Service struct {
	Name        string
	Description string
	Components  []string
	Output      OutputSettings
	Defaults    TopicDefaults
	Messages    MessageDefinitions
	}

	It then takes in all of the .yaml defined components and does the same thing
	
	type Component struct {
	Name        string
	Description string

	Sources     []Source
	Processors  []Processor
	Sinks       []Sink
	Views       []View
	ViewSources []ViewSource `yaml:"viewSources"`
	ViewSinks   []ViewSink   `yaml:"viewSinks"`

	Persistence *Persistence
	}

kafmesh-gen then continues, and feeds these Golang objects (a Service and its Components) into the "Generator"
This creates the directory defined in your services definition.yml file (in Enplug's case, outpath path: internal/definitions)

It also creates a Golang file for your definition.yml (your service) called `service.km.go` which in turn provides public functions calling code defined within your generated "components"
It also creates a enplug/internal/models directory containing proto files. These are generated off of whatever paths you gave your definition.yml file for messages.protofub.[path]
I'm assuming these proto files are used when communicating with Kafka.

Now, in the main method of your service's (enplug) code, you have a bunch of pre-made methods ready to use. You can 'balena.NewDeviceProcessor()' and 'definitions.Register_Balena_Device_Processor()
Your service is already hosting a grpc server for each processor to receive/send to kafka.

As your main method is running through and calling these methods, it's setting up local storage and topic for your processors and consumers


So you could generate a method that returns a service's configuration, but then you'd have to manually add code to get it to register. Unless you gave it a generic name and called it from your existing RPC method.




kafmesh/internal/services/discover.go




i need protoc-gen-go to kick off and spit out a new pb.go file into a place where i can actually use it

unless i can access the runner.service
