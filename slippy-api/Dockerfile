FROM harbor.mycarrier.tech/proxy-cache/golang:1.26 AS builder
WORKDIR /app
COPY ./go.mod ./go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /slippy-api

FROM harbor.mycarrier.tech/proxy-cache/alpine:3.23.0
RUN apk add --no-cache ca-certificates
WORKDIR /app

COPY --from=builder /slippy-api /usr/local/bin/slippy-api

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/slippy-api"]
