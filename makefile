export VERSION := $(shell gogitver)

build:
	docker build -t testing --target test .
	docker build -t syncromatics/kafmesh-discovery:${VERSION} --target final .

test: build
	mkdir -p artifacts/
	docker run -v $(PWD)/artifacts:/artifacts -v /var/run/docker.sock:/var/run/docker.sock testing
	cd artifacts && curl -s https://codecov.io/bash | bash

generate:
	mkdir -p internal/protos

	protoc -I docs/protos \
		docs/protos/kafmesh/ping/v1/*.proto \
		--go_out=plugins=grpc:./internal/protos

	protoc -I docs/protos \
		docs/protos/kafmesh/discover/v1/*.proto \
		--go_out=plugins=grpc:./internal/protos

	go generate ./...

build-local:
	go build -o ./artifacts/kafmesh-gen ./cmd/kafmesh-gen/main.go

ship:
	docker login --username ${DOCKER_USERNAME} --password ${DOCKER_PASSWORD}
	docker build -t ssyncromatics/kafmesh-discovery:${VERSION} --target final .
	docker push syncromatics/kafmesh-discovery:${VERSION} 