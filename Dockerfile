# One image for every binary. The container command selects which one runs:
#   /app/api serve | /app/api migrate up | /app/worker | /app/mockprovider
#
# No BuildKit-only syntax (such as RUN --mount), so the classic builder that Compose v1 uses
# builds it too. Dependencies are cached in their own layer instead.
FROM golang:1.27.4 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# tools/ (mock provider) is built when present.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/... $(test -d tools && echo ./tools/...)

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /app/
USER nonroot:nonroot
CMD ["/app/api", "serve"]
