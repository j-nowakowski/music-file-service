FROM golang:latest
WORKDIR /go/src
COPY . .

RUN true \
    && go get -d -v ./... \
    && go install -v ./... \
    && go build . \
    && go test -v ./...

CMD ["music-file-service"]
