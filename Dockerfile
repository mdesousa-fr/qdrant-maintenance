FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /qdrant-maintenance ./cmd/qdrant-maintenance

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /qdrant-maintenance /qdrant-maintenance
USER 65532:65532
ENTRYPOINT ["/qdrant-maintenance"]
CMD ["help"]
