# Musashi sub-transaction fixtures

## Issue #12 block

`5ba7b23a04aadcd104db62d67a840fa208342fdad749ee138a4579236579a5d7.cbor.hex`
is the full block requested by [issue #12](https://github.com/SundaeSwap-finance/sundae-sync-v2/issues/12),
supplied by Rodrigo from another sync-v2 instance's archive. The hex was moved
without modification. Its decoded bytes also match the local Dolos v1beta
`FetchBlock` response's `nativeBytes`.

Expected facts checked independently of Pallas using Python `cbor2` to locate
the original CBOR slices and `hashlib.blake2b(digest_size=32)` to hash them:

- Block header hash: `5ba7b23a04aadcd104db62d67a840fa208342fdad749ee138a4579236579a5d7`
- Height: 112280; slot: 2494559; era wrapper tag: 8 (Dijkstra).
- Raw length: 64637 bytes.
- Raw bytes SHA-256: `9cf4e069e2e0df6682952323099448e9ee74c48dce6950cf718b9777f1d45e7c`.
- 126 distinct top-level transactions; one sub-transaction, under parent index 123.
- Successful parent ID: `ca89430ca01c3202ce2cc8428c53f91a4f34d618421ca510dd39f3d76c540aca`.
- Sub-tx ID: `ede49f69c8e99d253d82b0b06d024be6435168cfeb1b960761370fe1bf5f8377`.
- Sub-tx output 0: 8000000 lovelace to address hex
  `70c323b9545afa6826b4121c048634ec168d78fa6f2349198169ff76f7`.
- Sub-tx output 1: 1043020 lovelace to address hex
  `60fc8b6e7cebcc4b49a05640f9a64f8a9f2c67109bddbd348ce41ba22a`.

The regression test decodes the full block, verifies 127 lookup rows including
the sub-tx's own outputs, checks that the original CBOR is uploaded unchanged,
and verifies rollback of all 127 rows in batches of 100 and 27.

## Smaller transaction fixture

`dijkstra-subtx.tx` is hex-encoded CBOR copied from
[Pallas at `4c294e6`](https://github.com/SundaeSwap-finance/pallas/blob/4c294e632fa5159bc65e82dec5007a62cf6f5112/test_data/dijkstra-subtx.tx).
Its [provenance](https://github.com/SundaeSwap-finance/pallas/blob/4c294e632fa5159bc65e82dec5007a62cf6f5112/test_data/dijkstra-fixtures.md#transactions-cut-from-the-chain)
records a real Musashi transaction at slot 853600 in block
`0c88b1e221beb608ed37dcfabed93a4a4e8f1ea0ec96f2d93b84c04816d5ddb4`.

This is a 728-byte **transaction**, not a full block. It contains the parent
body, witness set, auxiliary data, and success flag. Tests wrap the mapped
transaction in a synthetic RPC block; they do not claim that wrapper is the
original block.

Expected facts, checked independently of the RPC mapper using the original
CBOR body slices:

- Parent ID: `74e2116ca6e0c809f156aa062a9d4ee8b156322618846f1bdea5d9ad7a206a12`
- Sub-tx ID: `63ac433eefaed71cf0fa2c7134f82ea38f3c6c338b3d5129c686c47b44162691`
- Parent output 0: 1,903,631 lovelace.
- Sub-tx output 0: 3,000,000 lovelace.
- Raw bytes SHA-256: `f3b05fb3cf44d8f9087714fb1402b91eb9f0077a97a3f7a77b1b08fed27524af`.
