#!/usr/bin/env bash
#
# guard-cohort.sh — enforces cohort's import discipline.
#
# cohort is a provider- and domain-agnostic reconciliation core. Its whole
# value is that the SAME unmodified core compiles against any provider (AWS,
# Azure, …) and any domain (Slurm/MPI, Globus, agent transports). That only
# holds if the core never imports a cloud SDK or a scheduler — see doc.go's
# "IMPORT DISCIPLINE" block and API.md §8 ("The guard travels with cohort",
# which calls this check "not optional").
#
# This script is the standalone-repo home of the guard that previously lived in
# queuezero's Makefile. It fails (exit 1) if the transitive dependency graph of
# the module pulls in any forbidden package.
#
# Run locally:  make guard   (or)   ./scripts/guard-cohort.sh
set -euo pipefail

# Forbidden import-path substrings: cloud SDKs and schedulers. Anchored to the
# parts of the path that are unambiguous so we don't false-positive on, say, a
# stdlib path that happens to contain a version-looking segment.
FORBIDDEN='github.com/aws/aws-sdk-go|aws-sdk-go-v2|github.com/Azure/azure-sdk|cloud\.google\.com/go|k8s\.io/|hashicorp/nomad|/slurm'

# go list -deps prints the full transitive import set (one path per line),
# including the module's own packages. Grep the set for anything forbidden.
deps="$(go list -deps ./...)"

if echo "$deps" | grep -E "$FORBIDDEN" ; then
	echo
	echo "FAIL: cohort core imports a forbidden cloud SDK or scheduler package (see above)."
	echo "      The core must depend ONLY on the interfaces in ports.go — providers"
	echo "      and domains are supplied by consumers. See doc.go and API.md §8."
	exit 1
fi

echo "guard-cohort: OK — no cloud SDK or scheduler in the dependency graph."
