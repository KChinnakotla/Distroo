# --- build stage ---
FROM golang:1.22 AS build
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/kvnode ./cmd/kvnode
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/kvctl ./cmd/kvctl

# --- runtime stage ---
FROM alpine:3.20
RUN adduser -D app
USER app
WORKDIR /home/app
COPY --from=build /out/kvnode /usr/local/bin/kvnode
COPY --from=build /out/kvctl /usr/local/bin/kvctl
RUN mkdir -p /home/app/data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/kvnode"]
