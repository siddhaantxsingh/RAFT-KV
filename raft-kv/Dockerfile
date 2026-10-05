FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvserver ./cmd/kvserver \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvctl ./cmd/kvctl

FROM alpine:3.20
# Non-root, no shell tools beyond busybox; the binary is static (CGO off).
RUN adduser -D -u 10001 kv && mkdir /data && chown kv /data
COPY --from=build /out/ /usr/local/bin/
USER kv
VOLUME /data
EXPOSE 7000
HEALTHCHECK --interval=10s --timeout=2s CMD wget -qO- http://127.0.0.1:7000/healthz || exit 1
ENTRYPOINT ["kvserver", "-data", "/data"]
