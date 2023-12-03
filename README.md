# mvps

Log-structured, transactional virtual block device compatible with the
[NBD protocol](https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md).
`mvps` stands for "multi-versioned page store".

MVPS can store data on any S3-compatible object storage service, like Amazon S3,
Google Cloud Storage, Cloudflare R2, or MinIO. The local disk is only used as a
write buffer.

## Usage

Create a bucket on an S3-compatible object storage service. If running locally,
you can use `minio`.

Start the _transaction engine_:

```bash
export MVPS_TE_JWT_SECRET="insecure_token"
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export S3_ENDPOINT=http://localhost:8355

cargo run -p mvps-te --release -- \
  --listen 127.0.0.1:10809 \
  --image-store s3 \
  --image-store-s3-bucket mvps-image-store \
  --image-store-s3-prefix test/ \
  --buffer-store-path /path/to/buffer/store
```

Sign a JWT with the secret provided in environment variable
`MVPS_TE_JWT_SECRET`. Here's one you can use:

```
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZV9pZCI6IlRFU1RJTUctMDIubXZwcy1pbWFnZSIsImltYWdlX3NpemUiOjEwNzM3NDE4MjQsInBhZ2Vfc2l6ZV9iaXRzIjoxMiwiZXhwIjoxOTAxMTU0OTQxLCJjbGllbnRfaWQiOiJjOWVmZDk2Yy1kYTU5LTQzNTEtYjMxMS0zNTEyOTVmYTk4ZTIifQ.j-nR5Opn-pK6whrm8pJIgyItO5XvlYf0oy7RzR00T1Y
```

The payload of this JWT is:

```json
{
  "image_id": "TESTIMG-02.mvps-image",
  "image_size": 1073741824,
  "page_size_bits": 12,
  "exp": 1901154941,
  "client_id": "c9efd96c-da59-4351-b311-351295fa98e2"
}
```

Now, you can attach this virtual block device to your system:

```bash
nbd-client -N eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZV9pZCI6IlRFU1RJTUctMDIubXZwcy1pbWFnZSIsImltYWdlX3NpemUiOjEwNzM3NDE4MjQsInBhZ2Vfc2l6ZV9iaXRzIjoxMiwiZXhwIjoxOTAxMTU0OTQxLCJjbGllbnRfaWQiOiJjOWVmZDk2Yy1kYTU5LTQzNTEtYjMxMS0zNTEyOTVmYTk4ZTIifQ.j-nR5Opn-pK6whrm8pJIgyItO5XvlYf0oy7RzR00T1Y 127.0.0.1 10809 /dev/nbd0 -b 4096

mkfs.ext4 /dev/nbd0
mount /dev/nbd0 /mnt
```

Note that **you should never use the same image with different buffer stores**.
Doing so causes data corruption. An exception to this rule is when the previous
buffer store has become permanently inaccessible (e.g. deleted) - in this case,
you can use a new buffer store, and the data that is not yet flushed to object
storage in the previous buffer store is lost. Transactional consistency is
preserved.

## Garbage collection

Like any log-structured storage system, MVPS needs to perform background tasks
like checkpointing, compaction, and garbage collection. Checkpointing and
compaction are performed by the transaction engine. Garbage collection, due to
its global nature, is performed by a separate _garbage collector_ process,
`mvps-s3-gc`.

The transaction engine uses the object storage as an immutable store. Without
doing garbage collection, the bucket will grow indefinitely. `mvps-s3-gc` scans
the bucket for:

- Historic versions of images
- Objects that are no longer referenced by any image

and deletes them.

To run garbage collection once:

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export S3_ENDPOINT=http://localhost:8355

cargo run -p mvps-s3-gc -- \
  --bucket mvps-image-store --prefix test/ \
  --threshold-inactive-days 0
```

## Transactional extension

MVPS extends the NBD protocol with 4 custom commands:

```rust
pub const NBD_CMD_MVPS_BEGIN_TXN: u16 = 64;
pub const NBD_CMD_MVPS_COMMIT_TXN: u16 = 65;
pub const NBD_CMD_MVPS_ROLLBACK_TXN: u16 = 66;
pub const NBD_CMD_MVPS_LOCK_TXN: u16 = 67;
```

All writes issued between `NBD_CMD_MVPS_BEGIN_TXN` and
`NBD_CMD_MVPS_{COMMIT,ROLLBACK}_TXN` are atomically committed or rolled back.
When a transaction is active, reads follow the read-your-writes semantics.
