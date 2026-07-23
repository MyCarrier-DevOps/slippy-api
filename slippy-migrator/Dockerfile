FROM harbor.mycarrier.tech/proxy-cache/golang:1.26.5 AS builder
WORKDIR /app
COPY ./go.mod ./go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /slippy-migrator

FROM harbor.mycarrier.tech/proxy-cache/alpine:3.23.0
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 65532 slippy
WORKDIR /app

COPY --from=builder /slippy-migrator /usr/local/bin/slippy-migrator

# Run as a non-root user (defense-in-depth; pair with a pod securityContext).
USER 65532

# Runs once as a k8s Job (ArgoCD/Helm PreSync hook), applies the schema, and exits.
ENTRYPOINT ["/usr/local/bin/slippy-migrator"]
