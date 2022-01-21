FROM golang:1.17 as build

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

RUN apt update && apt install -y protobuf-compiler
RUN go get -u github.com/golang/protobuf/protoc-gen-go
RUN go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
RUN go get github.com/golang/mock/mockgen@latest

CMD go test -race -coverprofile=/artifacts/coverage.txt -covermode=atomic -p 1 ./...

# final image
FROM ubuntu:18.04 as final

WORKDIR /app

COPY --from=0 /build/discovery /app/

CMD ["/app/discovery"]
