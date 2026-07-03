FROM golang:1.26.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/exporter ./cmd/exporter

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/exporter /exporter
USER nonroot:nonroot
ENTRYPOINT ["/exporter"]
