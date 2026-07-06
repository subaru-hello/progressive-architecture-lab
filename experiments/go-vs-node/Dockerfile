# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o server main.go

FROM alpine:3.20
COPY --from=build /app/server /server
EXPOSE 3001
ENTRYPOINT ["/server"]
