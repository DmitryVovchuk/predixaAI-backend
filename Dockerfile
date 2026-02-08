FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/db-connector ./cmd/service

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
COPY --from=build /out/db-connector /app/db-connector
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/app/db-connector"]
