ENVTEST_K8S_VERSION = 1.27.1

.PHONY: test
test: test-unit

.PHONY: test-all
test-all: test-race test-integration

.PHONY: test-unit
test-unit:
	go test -mod=readonly ./pkg/...

.PHONY: test-race
test-race:
	go test -mod=readonly -race ./pkg/...

.PHONY: test-integration
test-integration: envtest
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test ./tests/integration -v

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)
ENVTEST ?= $(LOCALBIN)/setup-envtest

.PHONY: envtest
envtest: $(ENVTEST) ## Download envtest-setup locally if necessary.
$(ENVTEST): $(LOCALBIN)
	test -s $(LOCALBIN)/setup-envtest || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

