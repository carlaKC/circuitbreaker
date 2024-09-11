### Build backend
FROM golang:1.19-alpine AS build_backend

ARG BUILD_VERSION

WORKDIR /src

COPY *.go go.mod go.sum ./
COPY circuitbreakerrpc circuitbreakerrpc/

RUN go mod tidy
RUN go build -o /usr/bin/circuitbreaker -ldflags "-X main.BuildVersion=$BUILD_VERSION"

### Build an Alpine image
FROM alpine:3.16 AS alpine

# Update CA certs and install python for timestamp script
RUN apk add --no-cache ca-certificates python3 && rm -rf /var/cache/apk/*

# Copy over app binary
COPY --from=build_backend /usr/bin/circuitbreaker /usr/bin/circuitbreaker

# Copy in historical reputation data and script to adjust it
COPY historical_data/raw_data_csv /raw_data.csv
COPY historical_data/progress_timestamps.py /progress_timestamps.py

# Copy the entrypoint script into the container
COPY entrypoint.sh /entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /entrypoint.sh

COPY webui-build/ .

# Set the entrypoint script to run when the container launches
ENTRYPOINT ["/entrypoint.sh"]
