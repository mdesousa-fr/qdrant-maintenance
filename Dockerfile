FROM --platform=$BUILDPLATFORM golang:1.26.10 AS build
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

FROM build AS integration-build
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go test -c -tags=integration -o /s3-integration.test ./internal/s3

FROM gcr.io/distroless/static-debian12:debug-nonroot AS debug
COPY --from=build /qdrant-maintenance /qdrant-maintenance
COPY --from=integration-build /s3-integration.test /s3-integration.test
COPY --chmod=755 scripts/run-s3-integration.sh /run-s3-integration
USER 65532:65532
ENTRYPOINT ["/run-s3-integration"]
CMD ["-test.run=^TestNetApp$", "-test.v", "-test.timeout=15m"]

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /qdrant-maintenance /qdrant-maintenance
USER 65532:65532
ENTRYPOINT ["/qdrant-maintenance"]
CMD ["help"]
