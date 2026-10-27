FROM --platform=$BUILDPLATFORM golang:1.26.10 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=1.0.0
ARG GIT_COMMIT=development
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT}" -o /qdrant-maintenance ./cmd/qdrant-maintenance

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /qdrant-maintenance /qdrant-maintenance
USER 65532:65532
ENTRYPOINT ["/qdrant-maintenance"]
CMD ["help"]
