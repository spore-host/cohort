.PHONY: test vet guard check

# guard enforces the import discipline that makes cohort reusable: the core
# must import no cloud SDK and no scheduler (doc.go, API.md §8). Not optional.
guard:
	./scripts/guard-cohort.sh

vet:
	go vet ./...

test:
	go test -race ./...

# check runs everything CI runs, locally.
check: guard vet test
