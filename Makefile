
CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init:
	mkdir -p $(CONFIG_PATH)

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr $(CONFIG_PATH)

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.


$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: test
test: $(CONFIG_PATH)/model.conf $(CONFIG_PATH)/policy.csv
	go test -race ./... -v
	


.PHONY: install_grpc
install_grpc:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.8
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1


.PHONY: install_ssl
install_ssl:
	go install github.com/cloudflare/cfssl/cmd/cfssl@latest
	go install github.com/cloudflare/cfssl/cmd/cfssljson@latest