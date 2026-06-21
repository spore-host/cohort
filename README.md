# cohort

A reconciliation core for **named sets of identity-bearing entities** — cohorts —
that succeed, fail, and fast-fail *as a unit*, followed by a domain-defined
assembly phase. The single entity is the 1-cohort; an MPI job is a collective
cohort with an all-or-nothing barrier.

```
import "github.com/spore-host/cohort"
```

## Why this exists

The standard cloud toolbox (ASG, managed node groups, Batch, Kubernetes
Deployments) is built on abstraction-by-erasure: it works by throwing entity
identity away. `cohort` assumes the opposite — entities are named, placed,
stateful participants that must come up together, learn about each other, and
fail together. That assumption is the product.

## The two seams

`cohort` itself is provider- and domain-agnostic. It depends only on the
interfaces in `ports.go`:

- **Provider seam** — `Actuator` / `Observer` / `Classifier`, filled per cloud
  (e.g. an AWS implementation backed by EC2).
- **Domain seam** — `Enroller` / `Assembler`, filled per domain (Slurm/MPI,
  Globus, …).

A `Reconciler` is constructed with these and drives a cohort from intent to a
populated `Outcome` (one `Record` per entity — legibility is a non-negotiable:
"it didn't work" is never the outcome string). It is tested entirely against
fakes; it imports no cloud SDK and no scheduler.

## Status

`cohort` graduated from queuezero's `internal/cohort` into this standalone
module once two independent domains (MPI and Slurm) compiled against an
unmodified core. Its only dependency is `golang.org/x/sync`.

The exported API surface and the keep/unexport rationale live in
[`API.md`](API.md). This is **v0.x** — the interfaces are stable enough to build
against, but a change to `ports.go` is a coordinated, tagged release across
consumers; v1.0 is earned once the seam has held across a third independent
consumer.

## Develop

```
go test ./...
go vet ./...
```

No AWS, no Slurm, no network — the test suite is self-contained.
