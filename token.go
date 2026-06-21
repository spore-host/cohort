package cohort

import (
	"crypto/sha256"
	"encoding/hex"
)

// Token derives a deterministic idempotency token from the cluster, entity,
// and generation. This is the CANONICAL implementation — substrate.Token
// delegates here.
//
// Determinism is the entire point: re-issuing a mutation with the same token
// after an Ambiguous fault returns the already-created resource rather than
// creating a duplicate. A random token silently breaks the ambiguous-retry
// guarantee: RunInstances with a new token launches a second, unreaped instance.
//
// Format: "q0-" + first 16 bytes of SHA-256(cluster + NUL + entity + NUL + generation)
// as lowercase hex.
func Token(cluster, entity, generation string) string {
	h := sha256.Sum256([]byte(cluster + "\x00" + entity + "\x00" + generation))
	return "q0-" + hex.EncodeToString(h[:16])
}
