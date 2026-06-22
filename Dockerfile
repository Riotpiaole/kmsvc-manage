FROM golang:1.26 AS build
WORKDIR /src
ENV GOPRIVATE=forgejo.riotpiao.homelab.com
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/kmsvc-server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kmsvc-server /kmsvc-server
USER nonroot:nonroot
ENTRYPOINT ["/kmsvc-server"]
