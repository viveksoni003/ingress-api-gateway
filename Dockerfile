# syntax=docker/dockerfile:1

# --- build stage -----------------------------------------------------------
FROM golang:1.22-alpine AS build

WORKDIR /app
RUN apk add --no-cache git ca-certificates

# Copy sources and resolve dependencies. `go mod tidy` generates go.sum if it is
# not committed (the build stage has network access).
COPY . .
RUN go mod tidy

ARG VERSION=docker
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/gateway ./cmd/gateway

# --- runtime stage (distroless, non-root) ----------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/gateway /app/gateway
COPY --from=build /app/migrations /app/migrations
COPY --from=build /app/docs /app/docs

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/gateway"]
