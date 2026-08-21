# Copyright (c) 2026 The BFE Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# init project path
WORKROOT := $(shell pwd)
OUTDIR   := $(WORKROOT)/output
OS		 := $(shell go env GOOS)

# init environment variables
export PATH        := $(shell go env GOPATH)/bin:$(PATH)
export GO111MODULE := on

# init command params
GO           := go
GOBUILD      := $(GO) build
GOTEST       := $(GO) test
GOVET        := $(GO) vet
GOGET        := $(GO) get
GOGEN        := $(GO) generate
GOCLEAN      := $(GO) clean
GOINSTALL    := $(GO) install
GOFLAGS      := -race
STATICCHECK  := staticcheck
LICENSEEYE   := license-eye
PIP          := pip3
PIPINSTALL   := $(PIP) install

# init arch
ARCH := $(shell getconf LONG_BIT)
ifeq ($(ARCH),64)
	GOTEST += $(GOFLAGS)
endif

# init log-reader version
LOG_READER_VERSION ?= $(shell cat VERSION)
# init git commit id
GIT_COMMIT ?= $(shell git rev-parse HEAD)

# init log-reader packages
LOG_READER_PKGS := $(shell go list ./...)

# go install package
# $(1) package name
# $(2) package address
define INSTALL_PKG
	@echo installing $(1)
	$(GOINSTALL) $(2)
	@echo $(1) installed
endef

define PIP_INSTALL_PKG
	@echo installing $(1)
	$(PIPINSTALL) $(1)
	@echo $(1) installed
endef

# make, make all
all: prepare compile package

# make, make strip
strip: prepare compile-strip package

# make prepare, download dependencies
prepare: prepare-dep prepare-gen
prepare-dep:
prepare-gen:

# make compile, go build
compile: test build
build:
ifeq ($(OS),darwin)
	$(GOBUILD) -ldflags "-X main.version=$(LOG_READER_VERSION) -X main.commit=$(GIT_COMMIT)" -o log_reader ./main
else
	$(GOBUILD) -ldflags "-X main.version=$(LOG_READER_VERSION) -X main.commit=$(GIT_COMMIT) -extldflags=-static" -o log_reader ./main
endif

# make compile-strip, go build without symbols and DWARFs
compile-strip: test build-strip
build-strip:
ifeq ($(OS),darwin)
	$(GOBUILD) -ldflags "-X main.version=$(LOG_READER_VERSION) -X main.commit=$(GIT_COMMIT) -s -w" -o log_reader ./main
else
	$(GOBUILD) -ldflags "-X main.version=$(LOG_READER_VERSION) -X main.commit=$(GIT_COMMIT) -extldflags=-static -s -w" -o log_reader ./main
endif

# make test, test your code
test: test-case vet-case
test-case:
	$(GOTEST) -cover ./...
vet-case:
	${GOVET} ./...

# make coverage for codecov
coverage:
	echo -n > coverage.txt
	for pkg in $(LOG_READER_PKGS) ; do $(GOTEST) -coverprofile=profile.out -covermode=atomic $${pkg} && cat profile.out >> coverage.txt; done

# make package
package:
	mkdir -p $(OUTDIR)/bin
	mv log_reader  $(OUTDIR)/bin
	cp -r conf $(OUTDIR)

# make release, build cross-compiled release packages
# produces: dist/log-reader_$(VERSION)_linux_amd64.tar.gz, linux_arm64, windows_amd64
# NOTE: darwin is not supported (no register_signal_darwin.go)
release: prepare
	@echo "Building release packages for log-reader $(LOG_READER_VERSION)..."
	@for platform in \
		"linux/amd64" \
		"linux/arm64" \
		"windows/amd64"; do \
		GOOS=$${platform%/*}; \
		GOARCH=$${platform#*/}; \
		PKG_DIR="dist/log-reader_$(LOG_READER_VERSION)_$${GOOS}_$${GOARCH}"; \
		BIN_NAME="log-reader"; \
		LDFLAGS="-X main.version=$(LOG_READER_VERSION) -X main.commit=$(GIT_COMMIT)"; \
		if [ "$${GOOS}" = "windows" ]; then \
			BIN_NAME="log-reader.exe"; \
		fi; \
		echo "  -> Building $$GOOS/$$GOARCH..."; \
		rm -rf "$${PKG_DIR}"; \
		mkdir -p "$${PKG_DIR}/bin"; \
		CGO_ENABLED=0 GOOS=$${GOOS} GOARCH=$${GOARCH} $(GOBUILD) -ldflags "$${LDFLAGS}" -o "$${PKG_DIR}/bin/$${BIN_NAME}" ./main; \
		cp -r conf "$${PKG_DIR}/"; \
		cp readme.txt "$${PKG_DIR}/README.md"; \
		cp LICENSE "$${PKG_DIR}/"; \
		tar -czf "$${PKG_DIR}.tar.gz" -C "$${PKG_DIR}" .; \
		rm -rf "$${PKG_DIR}"; \
		echo "  -> dist/log-reader_$(LOG_READER_VERSION)_$${GOOS}_$${GOARCH}.tar.gz done"; \
	done
	@echo "Release packages built successfully."

# make deps
deps:
	$(call PIP_INSTALL_PKG, pre-commit)
	$(call INSTALL_PKG, staticcheck, honnef.co/go/tools/cmd/staticcheck)
	$(call INSTALL_PKG, license-eye, github.com/apache/skywalking-eyes/cmd/license-eye@latest)

# make precommit, enable autoupdate and install with hooks
precommit:
	pre-commit autoupdate
	pre-commit install --install-hooks

# make check
check:
	$(STATICCHECK) ./...

# make license-check, check code file's license declaration
license-check:
	$(LICENSEEYE) header check

# make license-fix, fix code file's license declaration
license-fix:
	$(LICENSEEYE) header fix

# make clean
clean:
	$(GOCLEAN)
	rm -rf dist/
	rm -rf $(OUTDIR)
	rm -rf $(WORKROOT)/log_reader
	rm -rf $(GOPATH)/pkg/linux_amd64

# avoid filename conflict and speed up build 
.PHONY: all prepare compile test package release clean build strip compile-strip build-strip test-case vet-case coverage deps precommit check license-check license-fix prepare-dep prepare-gen
