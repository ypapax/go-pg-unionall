# Compile app binary
FROM golang:latest as build-env

WORKDIR /go/src/app
ENV GO111MODULE=on

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY *.go .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o ~/app ./*.go

#FROM alpine
#RUN apk add wamerican
FROM ubuntu
RUN apt-get update
RUN apt-get install --reinstall wamerican -y

COPY --from=build-env /root/app /

CMD ["/app"]