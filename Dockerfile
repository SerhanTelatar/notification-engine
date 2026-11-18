# One image for every binary. The container command selects which one runs:
#   /app/api serve | /app/api migrate up | /app/worker | /app/mockprovider
FROM golang:1.27.4 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
# tools/ (mock provider) is built when present.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/... $(test -d tools && echo ./tools/...)

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /app/
USER nonroot:nonroot
CMD ["/app/api", "serve"]
