FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git protobuf-dev protobuf make

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN make build

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/bin/* /usr/local/bin/

EXPOSE 7233

CMD ["aetherd", "--port", "7233", "--node-id", "aether-node-1"]
