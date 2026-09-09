# Build context must be the parent directory containing BOTH repositories (see compose.yaml).
FROM oven/bun:1 AS frontend
WORKDIR /front
COPY FileCabinetFront/package.json FileCabinetFront/bun.lock ./
RUN bun install --frozen-lockfile
COPY FileCabinetFront/index.html FileCabinetFront/tsconfig.json FileCabinetFront/vite.config.ts ./
COPY FileCabinetFront/src ./src
RUN bun run build

FROM golang:1.24 AS backend
WORKDIR /app
COPY FileCabinet/go.mod FileCabinet/go.sum ./
RUN go mod download
COPY FileCabinet/main.go ./
COPY FileCabinet/internal ./internal
COPY --from=frontend /front/dist ./public
RUN CGO_ENABLED=0 go build -trimpath -o /filecabinet .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=backend /filecabinet /filecabinet
EXPOSE 8080
ENTRYPOINT ["/filecabinet"]
