build:
	docker build -t testing --target test .

test: build
	mkdir -p artifacts/
	docker run -v $(PWD)/artifacts:/artifacts -v /var/run/docker.sock:/var/run/docker.sock testing
	cd artifacts && curl -s https://codecov.io/bash | bash

proto-lint:
	docker run -v "$(PWD)/docs/protos:/work" uber/prototool:latest prototool lint

generate: proto-lint
	mkdir -p internal/protos
	docker run -v "$(PWD)/docs/protos:/work" -v $(PWD):/output uber/prototool:1.8.1 prototool generate
	
	go get github.com/rakyll/statik
	statik -f -src=./docs/migrations -dest=./internal/storage
	
	go get github.com/99designs/gqlgen
	go run github.com/99designs/gqlgen generate --config docs/graphql/gqlgen.yml
	
	go get github.com/vektah/dataloaden
	go generate ./...

build-local:
	go build -o ./artifacts/kafmesh-gen ./cmd/kafmesh-gen/main.go

ship:
	docker login --username ${DOCKER_USERNAME} --password ${DOCKER_PASSWORD}
	docker build -t syncromatics/kafmesh-discovery:${VERSION} --target final .
	docker push syncromatics/kafmesh-discovery:${VERSION}
