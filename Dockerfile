FROM golang:1.17-alpine3.14 AS build-env
RUN apk add build-base

WORKDIR /build

COPY app/go.mod .
COPY app/go.sum .

RUN go mod download

COPY app/*.go .

RUN go build .

FROM alpine

WORKDIR /app

COPY --from=build-env /build/app ./app

EXPOSE 8080

ENTRYPOINT ["./app"]
