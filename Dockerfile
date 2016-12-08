FROM golang 
ADD . /go/src/github.com/juju227/databalancer
RUN go install github.com/juju227/databalancer 
ENTRYPOINT /go/bin/databalancer 
EXPOSE 8080
