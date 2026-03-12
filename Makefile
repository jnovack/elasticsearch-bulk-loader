SHELL := /bin/sh

DOCKERFILE ?= build/package/Dockerfile

# Mirrors .github/workflows/docker-image.yml build args with local defaults.
APPLICATION ?= $(notdir $(firstword $(wildcard cmd/*)))
BUILD_RFC3339 ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
DESCRIPTION ?= "elasticsearch bulk loader"
PACKAGE ?= $(shell git config --get remote.origin.url 2>/dev/null | sed -e 's/^git@github.com://' -e 's/^https:\/\/github.com\///' -e 's/\.git$$//')
REVISION ?= $(shell git rev-parse HEAD 2>/dev/null || echo local)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo local)

# Image naming is overridable and defaults to PACKAGE:VERSION when PACKAGE is available.
DOCKER_ORGANIZATION ?= $(word 1,$(subst /, ,$(PACKAGE)))
DOCKER_REPOSITORY ?= $(word 2,$(subst /, ,$(PACKAGE)))
IMAGE ?= $(if $(and $(DOCKER_ORGANIZATION),$(DOCKER_REPOSITORY)),$(DOCKER_ORGANIZATION)/$(DOCKER_REPOSITORY):$(VERSION),$(APPLICATION):$(VERSION))

.PHONY: help docker-build print-vars

help:
	@echo "Targets:"
	@echo "  make docker-build  Build Docker image using workflow-compatible build args"
	@echo "  make print-vars    Show resolved Docker build variables"

print-vars:
	@echo "DOCKERFILE=$(DOCKERFILE)"
	@echo "IMAGE=$(IMAGE)"
	@echo "APPLICATION=$(APPLICATION)"
	@echo "BUILD_RFC3339=$(BUILD_RFC3339)"
	@echo "DESCRIPTION=$(DESCRIPTION)"
	@echo "PACKAGE=$(PACKAGE)"
	@echo "REVISION=$(REVISION)"
	@echo "VERSION=$(VERSION)"

docker-build:
	docker buildx build \
		--file $(DOCKERFILE) \
		--build-arg APPLICATION=$(APPLICATION) \
		--build-arg BUILD_RFC3339=$(BUILD_RFC3339) \
		--build-arg DESCRIPTION=$(DESCRIPTION) \
		--build-arg PACKAGE=$(PACKAGE) \
		--build-arg REVISION=$(REVISION) \
		--build-arg VERSION=$(VERSION) \
		--tag $(IMAGE) \
		.
