#BIRDCATCHER_VERSION?=$(shell git describe --tags)

all: bin/andrewd

bin:
	mkdir -p bin

bin/andrewd: bin */*.go
	go build -o bin/andrewd cmd/andrewd.go

get:
	go get -t ./...

fmt:
	go fmt ./...

install: all
	cp bin/andrewd $(DESTDIR)/usr/bin/andrewd

develop: all
	ln -f -s `pwd`/bin/* -t /usr/local/bin/

test:
	@test -z "$(shell find . -name '*.go' | xargs gofmt -l)" || (echo "Need to run 'go fmt ./...'"; exit 1)
	go vet ./...
	go test ./...
