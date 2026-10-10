FROM golang:1.25.0-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY tests/delivery ./tests/delivery
RUN CGO_ENABLED=0 go test -c -o /delivery.test ./tests/delivery && CGO_ENABLED=0 go test -c -o /storagepool.test ./internal/storagepool

FROM nginx:1.30.5-alpine
COPY --from=build /delivery.test /usr/local/bin/delivery.test
COPY --from=build /storagepool.test /usr/local/bin/storagepool.test
COPY deploy/nginx.conf /etc/nginx/nginx.conf
COPY deploy/snippets /etc/nginx/drop
USER 65534:65534
ENTRYPOINT ["/bin/sh", "-c", "/usr/local/bin/delivery.test -test.v && /usr/local/bin/storagepool.test -test.v -test.run ^TestPrimaryCacheNginx$"]
