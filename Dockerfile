# Install dependencies only when needed
FROM node:19.6.0-alpine AS build_frontend
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
RUN apk add --no-cache libc6-compat
WORKDIR /app
COPY web .
RUN yarn install --frozen-lockfile --network-timeout 1000000 && yarn build-export

### Build backend
FROM golang:1.19-alpine AS build_backend

ARG BUILD_VERSION

WORKDIR /src

COPY *.go go.mod go.sum ./
COPY circuitbreakerrpc circuitbreakerrpc/
COPY --from=build_frontend /webui-build/ webui-build/

RUN go install -ldflags "-X main.BuildVersion=$BUILD_VERSION"

### Build an Alpine image
FROM alpine:3.16 as alpine

# Update CA certs and install python for timestamp script
RUN apk add --no-cache ca-certificates python3 && rm -rf /var/cache/apk/*

# Copy over app binary
COPY --from=build_backend /go/bin/circuitbreaker /usr/bin/circuitbreaker

# Copy in historical reputation data and script to adjust it
COPY historical_data/raw_data_csv /raw_data.csv
COPY historical_data/progress_timestamps.py /progress_timestamps.py

# Copy the entrypoint script into the container
COPY entrypoint.sh /entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /entrypoint.sh

# Set the entrypoint script to run when the container launches
ENTRYPOINT ["/entrypoint.sh"]
