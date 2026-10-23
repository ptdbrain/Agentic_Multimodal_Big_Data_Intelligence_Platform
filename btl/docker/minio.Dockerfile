# Build the real upstream MinIO release; no substitute S3 implementation.
FROM golang:1.21-alpine AS build
WORKDIR /src
ADD --checksum=sha256:fd52b7d67aab1a540b8ede7b50674f0b1ce29dbb4344598f3801125396564c9d https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2024-03-30T09-41-56Z /src/source.tar.gz
RUN tar -xzf source.tar.gz --strip-components=1 && rm source.tar.gz
RUN CGO_ENABLED=0 GOMAXPROCS=2 go build -p 2 -trimpath -ldflags='-s -w' -o /out/minio .
FROM alpine:3.19
LABEL org.opencontainers.image.source="https://github.com/minio/minio"
LABEL org.opencontainers.image.version="RELEASE.2024-03-30T09-41-56Z-source-build"
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["/usr/local/bin/minio"]
