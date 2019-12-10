FROM golang:1.13.1 as build

WORKDIR /build

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . .

RUN go vet ./...

RUN go get -u golang.org/x/lint/golint

RUN golint -set_exit_status ./...

#testing
FROM build as test

ENV GOPATH=/go
RUN apt update && apt install -y protobuf-compiler
RUN go get -u github.com/golang/protobuf/protoc-gen-go

CMD go test -race -coverprofile=/artifacts/coverage.txt -covermode=atomic ./...
