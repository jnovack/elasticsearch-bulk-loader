# syntax=docker/dockerfile:1.7

# Create a minimal container to run a Golang static binary
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
WORKDIR /go/src/app

# Create appuser.
# See https://stackoverflow.com/a/55757473/12429735
ENV USER=appuser
ENV UID=10001
ARG TARGETOS
ARG TARGETARCH
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

RUN --mount=type=cache,target=/var/cache/apk \
    apk update && apk add git ca-certificates

COPY go.mod .
COPY go.sum .
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

ARG APPLICATION="go-executable"
ARG BUILD_RFC3339="1970-01-01T00:00:00Z"
ARG PACKAGE="user/repo"
ARG REVISION="local"
ARG VERSION="dirty"
ARG GO_LDFLAGS="-w -s \
    -X main.buildRFC3339=${BUILD_RFC3339} \
    -X main.revision=${REVISION} \
    -X main.version=${VERSION} \
    "

# Build
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags "${GO_LDFLAGS}" -o /go/bin/${APPLICATION} ./cmd/${APPLICATION}

# Copy binary to single-serve container
FROM scratch
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group
USER appuser:appuser

ARG APPLICATION="go-executable"
ARG BUILD_RFC3339="1970-01-01T00:00:00Z"
ARG DESCRIPTION="no description"
ARG PACKAGE="user/repo"
ARG REVISION="local"
ARG VERSION="dirty"

LABEL org.opencontainers.image.ref.name="${PACKAGE}" \
    org.opencontainers.image.created=$BUILD_RFC3339 \
    org.opencontainers.image.authors="Justin J. Novack <jnovack@gmail.com>" \
    org.opencontainers.image.documentation="https://github.com/${PACKAGE}/README.md" \
    org.opencontainers.image.description="${DESCRIPTION}" \
    org.opencontainers.image.licenses="MIT" \
    org.opencontainers.image.source="https://github.com/${PACKAGE}" \
    org.opencontainers.image.revision=$REVISION \
    org.opencontainers.image.version=$VERSION \
    org.opencontainers.image.url="https://hub.docker.com/r/${PACKAGE}/"

COPY --from=build /go/bin/${APPLICATION} /app

EXPOSE 80

ENTRYPOINT ["/app"]
