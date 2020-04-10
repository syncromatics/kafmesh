#export VERSION := $(shell gogitver)
DOCKER_REPOSITORY = harbor.syncromatics.com/infrastructure

build:
	docker build -t testing --target test .
	docker build -t $(DOCKER_REPOSITORY)/kafmesh-discover --target discover .

test: build
	mkdir -p artifacts/
	docker run -v $(PWD)/artifacts:/artifacts -v /var/run/docker.sock:/var/run/docker.sock testing
	cd artifacts && curl -s https://codecov.io/bash | bash

proto-lint:
	docker run -v "$(PWD)/docs/protos:/work" uber/prototool:latest prototool lint

generate: proto-lint
	mkdir -p internal/protos
	docker run -v "$(PWD)/docs/protos:/work" -v $(PWD):/output -u `id -u $(USER)`:`id -g $(USER)` uber/prototool:latest prototool generate
	go generate ./...

build-local:
	go build -o ./artifacts/kafmesh-gen ./cmd/kafmesh-gen/main.go

ship:
#	docker login --username ${DOCKER_USERNAME} --password ${DOCKER_PASSWORD}
#	docker build -t ssyncromatics/kafmesh-discover:${VERSION} --target final .
#	docker push syncromatics/kafmesh-discover:${VERSION}
	docker push $(DOCKER_REPOSITORY)/kafmesh-discover