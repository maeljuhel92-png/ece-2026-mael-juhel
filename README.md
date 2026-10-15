# Lab 2: Object storage with S3

## Summary

- Configured S3 access and used my personal bucket.
- Uploaded, listed, downloaded, renamed and deleted objects with `aws s3` and `s5cmd`.
- Tested metadata, presigned URLs, multipart upload, versioning, lifecycle rules and access control.
- Uploaded `users.csv` and `orders.csv` to `bronze/` from a Kubernetes Job.

## Questions

### Why are the credentials stored in a Secret and not in a ConfigMap?

A ConfigMap is plain text and meant for non-sensitive settings. A Secret is designed for sensitive data: access can be
restricted with RBAC, it can be encrypted at rest, and its values are not displayed by default.

### What happens if the Job runs again tomorrow? How would production provide credentials?

The Onyxia credentials are temporary, so they will have expired and the Job will fail with an authentication error.
In production, credentials are issued and renewed automatically, for example with a workload identity linked to the
service account (IAM role for service accounts) or a secret manager like Vault.

### How would you turn this Job into a daily ingestion?

Use a Kubernetes `CronJob` with a schedule such as `0 2 * * *`, and move the Job spec under `jobTemplate`. Also use
credentials that do not expire and write to a date-based prefix (e.g. `bronze/users/date=2026-09-14/`) so each run
keeps its own data.