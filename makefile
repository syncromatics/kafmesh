build:
	docker build -t testing --target test .

test: build
	mkdir -p artifacts/
	docker run -v $(PWD)/artifacts:/artifacts -v /var/run/docker.sock:/var/run/docker.sock testing
	cd artifacts && curl -s https://codecov.io/bash | bash

generate-protos:
	mkdir -p internal/protos
	protoc -I docs/protos \
		docs/protos/kafmesh/ping/v1/*.proto \
		--go_out=plugins=grpc:./internal/protos
	protoc -I docs/protos \
        docs/protos/kafmesh/discover/v1/*.proto \
    	--go_out=plugins=grpc:./internal/protos

build-local:
	go build -o ./artifacts/kafmesh-gen ./cmd/kafmesh-gen/main.go