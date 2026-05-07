# Segmented Prefix Fingerprints

## Why this exists

Current partial-cache candidate selection relies mostly on a short leading normalized prefix plus system-hash hints. That is already useful, but it can still miss good reuse opportunities when very large prompts share most of their real context outside the first short token window.

This follow-up step is intentionally deferred because the current system is already usable. If later we decide partial-hit accuracy is not good enough, this is the next logical upgrade.

## Problem it solves

Large prompts from Qwen Code often contain:

- a volatile environment preamble
- repeated tool/context scaffolding
- a large shared middle section
- only a small changing tail

A single short prefix fingerprint overweights the beginning of the prompt. That can cause two problems:

- false negatives: a strong reusable bin is ignored because the useful shared context starts later
- false positives: two prompts look similar at the front but diverge in the important task body

## Proposed upgrade

Store multiple deterministic prompt sketches per cache entry instead of only one short prefix.

Suggested sketches:

- first 256 normalized tokens
- first 1024 normalized tokens
- middle sample window
- last static-context window before the request-specific tail
- a sparse sampled signature across the full normalized prompt

At request time, compute the same sketches and combine them into a richer similarity score.

## Expected benefits

- better partial-hit accuracy on 60k-70k token prompts
- fewer wasted restores from weak front-only matches
- better reuse when the large stable context lives in the middle of the prompt

## Constraints

- keep hashing cooperative and chunked
- avoid large extra allocations in Node.js
- stay deterministic and local first; do not require embeddings or external vector infra
- preserve the current exact-hit path as the fast path

## When to revisit

Revisit this only if one of these becomes true:

- partial-hit rate is lower than expected on real workloads
- many partial restores show low actual savings
- logs suggest front-only prefix matching is choosing weak candidates

## Implementation outline

1. Extend request fingerprint generation to emit several prompt sketches.
2. Persist those sketches in metadata.json.
3. Update candidate scoring to blend prefix, middle, and sampled signatures.
4. Compare improved partial-hit savings against the current scorer before keeping it.

## Important note about existing bins

Existing bins can be kept. They do not need to be deleted.

- Old bins can still be used for exact hits.
- Old bins can be hydrated into metadata from filename and file stats.
- Old bins without prompt-derived sketches will not become strong segmented-match candidates until they are recreated or refreshed under the newer metadata model.
