SHELL := /bin/bash

IMAGE ?= man4ish/omnibioai-tool-runtime
TAG ?= latest
PLATFORMS ?= linux/amd64,linux/arm64

.PHONY: help
help:
	@echo "Targets:"
	@echo "  make fmt            - format (ruff)"
	@echo "  make lint           - lint (ruff)"
	@echo "  make test           - run unit tests (pytest)"
	@echo "  make build          - docker build (local arch)"
	@echo "  make buildx         - docker buildx multi-arch"
	@echo "  make push           - docker push (TAG)"
	@echo "  make buildx-push    - docker buildx multi-arch + push"
	@echo "  make run-local      - run echo_test locally (no upload)"
	@echo "  make run-s3         - run echo_test to S3 (requires AWS creds)"
	@echo "  make run-azure      - run echo_test to Azure Blob (requires AZURE_* env)"

.PHONY: fmt
fmt:
	python -m pip install -q ruff
	ruff format .

.PHONY: lint
lint:
	python -m pip install -q ruff
	ruff check .

.PHONY: test
test:
	python -m pip install -q pytest
	pytest -q

.PHONY: build
build:
	docker build -t $(IMAGE):$(TAG) .

.PHONY: buildx
buildx:
	docker buildx build --platform $(PLATFORMS) -t $(IMAGE):$(TAG) .

.PHONY: push
push:
	docker push $(IMAGE):$(TAG)

.PHONY: buildx-push
buildx-push:
	docker buildx build --platform $(PLATFORMS) -t $(IMAGE):$(TAG) --push .

.PHONY: run-local
run-local:
	docker run --rm \
	  -e TOOL_ID=echo_test \
	  -e RUN_ID=r1 \
	  -e INPUTS_JSON='{"text":"hello local"}' \
	  -e RESOURCES_JSON='{}' \
	  $(IMAGE):$(TAG)

.PHONY: run-s3
run-s3:
	@if [[ -z "$$RESULT_URI" ]]; then echo "Set RESULT_URI=s3://bucket/prefix"; exit 2; fi
	docker run --rm \
	  -e TOOL_ID=echo_test \
	  -e RUN_ID=r1 \
	  -e INPUTS_JSON='{"text":"hello s3"}' \
	  -e RESOURCES_JSON='{}' \
	  -e RESULT_URI="$$RESULT_URI" \
	  -e AWS_PROFILE="$$AWS_PROFILE" \
	  -v $$HOME/.aws:/root/.aws:ro \
	  $(IMAGE):$(TAG)

.PHONY: run-azure
run-azure:
	@if [[ -z "$$RESULT_URI" ]]; then echo "Set RESULT_URI=azureblob://account/container/prefix"; exit 2; fi
	@if [[ -z "$$AZURE_AUTH" ]]; then echo "Set AZURE_AUTH=managed_identity OR connection_string"; exit 2; fi
	docker run --rm \
	  -e TOOL_ID=echo_test \
	  -e RUN_ID=r1 \
	  -e INPUTS_JSON='{"text":"hello azure"}' \
	  -e RESOURCES_JSON='{}' \
	  -e RESULT_URI="$$RESULT_URI" \
	  -e AZURE_AUTH="$$AZURE_AUTH" \
	  -e AZURE_STORAGE_CONNECTION_STRING="$$AZURE_STORAGE_CONNECTION_STRING" \
	  $(IMAGE):$(TAG)
