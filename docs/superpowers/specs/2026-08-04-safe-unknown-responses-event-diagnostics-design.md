# Safe Unknown Responses Event Diagnostics

## Problem

The Claude-to-Codex bridge rejects unrecognized Responses SSE event types with
`invalid upstream Responses event (unknown)`. The forensic manifest records only
`typed_decode_error`, while full artifacts may be removed when the turn contains
credential-shaped data. As a result, the exact event type needed for a compatibility
fix is lost.

## Design

Before moving an SSE payload into the typed decoder, derive a diagnostic event label
from the named SSE `event` field or, when absent, from the JSON `type` field. Preserve
the existing mismatch and missing-type validation in the decoder.

Only event labels matching the complete grammar `[A-Za-z0-9._-]{1,128}` and passing
the existing protocol credential-shape scan may be stored verbatim. Any other
non-empty label is represented by a stable SHA-256 digest with a fixed `sha256:`
prefix. Missing labels remain `missing`. The diagnostic label is stored in the
manifest's existing streaming failure `event_kind` field as
`typed_decode_error:<safe-label>`, so it survives full-artifact suppression without
changing the evidence format.

No payload fields, SSE data, prompts, tool arguments, response text, or raw invalid
labels are added to application logs or manifests.

## Error Flow

1. Parse the SSE block into its optional named event and JSON payload.
2. Derive the safe diagnostic label without consuming either value.
3. Run the existing strict typed decoder unchanged.
4. On typed decode failure, record `typed_decode_error:<safe-label>` in the failure
   context, then return the existing client-visible error.
5. Continue using the existing evidence redaction and artifact suppression logic.

## Tests

- A recognized-format unknown event records its event type in the failure manifest
  context while the client error remains payload-free.
- A malicious or malformed event label is represented only by a SHA-256 digest; its
  original value is absent from the failure context and client error.
- Existing typed stream tests continue to pass, including credential non-disclosure
  and supported-event behavior.

## Scope

This change adds diagnostic metadata only. It does not accept, ignore, translate, or
retry any new Responses event type, and it does not change evidence retention.
