FROM golang:1.13

RUN mkdir /app
ADD . /app/
WORKDIR /app

RUN go build -o producer cmd/producer/main.go
RUN go build -o consumer cmd/consumer/main.go

CMD ["app"]