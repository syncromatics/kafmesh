FROM golang:1.13 as build

WORKDIR /build

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . .

RUN go vet ./...

RUN go get -u golang.org/x/lint/golint

RUN golint -set_exit_status ./...

RUN go build -o ./discovery ./cmd/kafmesh-discovery

#testing
FROM build as test

ENV GOPATH=/go
RUN apt update && apt install -y protobuf-compiler
RUN go get -u github.com/golang/protobuf/protoc-gen-go
RUN go get github.com/golang/mock/mockgen@latest

CMD go test -race -coverprofile=/artifacts/coverage.txt -covermode=atomic ./...

# final image
FROM ubuntu:18.04 as final

WORKDIR /app

COPY --from=0 /build/discovery /app/

CMD ["/app/discovery"]
