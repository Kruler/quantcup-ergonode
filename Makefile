GOPATH := $(shell pwd)

all: quantcup-ergonode

quantcup-ergonode:
	GOPATH=$(GOPATH) go get -d
	GOPATH=$(GOPATH) go build -o $@

clean:
	GOPATH=$(GOPATH) go clean
	${RM} -r pkg/ src/

.PHONY: quantcup-ergonode
