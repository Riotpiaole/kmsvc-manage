FROM golang:1.26 AS build
WORKDIR /src
ENV GOPRIVATE=forgejo.riotpiao.homelab.com
# forgejo.riotpiao.homelab.com's cert is signed by a homelab-private CA, not
# a public one -- without this, `go mod download` can't fetch kmsvc-proto.
COPY hack/homelab-ca.pem /usr/local/share/ca-certificates/homelab-ca.crt
RUN update-ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /out/kmsvc-server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/kmsvc-server /kmsvc-server
USER nonroot:nonroot
ENTRYPOINT ["/kmsvc-server"]
