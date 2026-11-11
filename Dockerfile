FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:08bcb99a38f85a265f9a93d1d3323e2440eec333380f3ea26ee0dec9264bb41c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=1.0.0
ARG GIT_COMMIT=development
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT}" -o /qdrant-maintenance ./cmd/qdrant-maintenance

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /qdrant-maintenance /qdrant-maintenance
USER 65532:65532
ENTRYPOINT ["/qdrant-maintenance"]
CMD ["help"]
