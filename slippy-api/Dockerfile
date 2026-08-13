FROM harbor.mycarrier.tech/proxy-cache/golang:1.26.5 AS builder
WORKDIR /app
COPY ./go.mod ./go.sum ./
RUN go mod download
COPY . .
# -trimpath strips local filesystem paths from the binary; -s -w drops the symbol and
# DWARF tables. Both shrink what a leaked or pulled image discloses about the build host.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /slippy-api

FROM harbor.mycarrier.tech/proxy-cache/alpine:3.23.0
RUN apk add --no-cache ca-certificates
WORKDIR /app

COPY --from=builder /slippy-api /usr/local/bin/slippy-api

# Run as a non-root, non-login user. The process needs no write access anywhere — it binds
# 8080 (unprivileged) and holds all state in Postgres — so this costs nothing and takes
# root out of the container that carries both API keys and the GitHub App private key in
# its environment. Numeric UID/GID so a securityContext with runAsNonRoot can verify it
# without resolving /etc/passwd.
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S nonroot -G nonroot
USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/slippy-api"]
