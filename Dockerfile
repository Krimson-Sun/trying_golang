ARG VERSION=1.22
FROM golang:$VERSION

ENV GOPATH=/
ENV GOSUMDB="off"

COPY ./ ./

RUN go build -o sosiska .

CMD ["./sosiska"]