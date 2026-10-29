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


# Lab 6 : Introduction to Spark's RDD API

## 1. RDD creation and partitions

`textFile()` returns a small number of partitions for these files, and `repartition(8)` then gives exactly 8.

### What decides the number of partitions `textFile()` picks by default, for a file this size?

`textFile(path, minPartitions)` uses `minPartitions = min(defaultParallelism, 2)` when none is given. The real count then comes from Hadoop's input splits, computed from the file size and the split size. A small file fits in one or two splits, so Spark ends up with about 2 partitions. A very large file would get roughly one per block.

### `repartition()` vs `coalesce()`: which one shuffles?

`repartition(n)` always performs a full shuffle and can increase or decrease the partition count. `coalesce(n)` merges existing partitions without a shuffle by default, so it is cheaper, but it can only reduce the count (unless `shuffle=True`) and can leave partitions unbalanced.

## 2. Lazy evaluation and lineage

`toDebugString()` only prints the lineage (file read, then `filter`), and no job runs until `count()` is called. Each `count()` appears as a job in the Spark UI.

### Does a second `orders_data.count()` show a new stage in the Spark UI? Why?

Yes, each action submits a new job with its own stage. `orders_data` is not cached, so Spark recomputes the whole lineage (read the file, then `filter`) every time. Transformations only record the lineage, and nothing is kept between actions unless it is persisted.

### At which point would `.cache()` change that answer?

Calling `orders_data.cache()` before the first `count()`. The first `count()` computes the RDD and keeps the partitions in memory, and the second `count()` reads from memory (stages skipped or much faster in the UI). Calling `.cache()` after the first `count()` would not help for that run: caching is itself lazy, so the data is only stored the next time an action computes it.

## 3. The cost of no schema: the multiline address

`users_lines.count()` is higher than the real number of users, because each address takes a second physical line. After the UUID filter, `users_records.count()` equals the number of users from the DataFrame lab.

### Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?

`textFile()` splits the file on every newline character. It has no notion of quoting or of a record spanning several lines, so the second line of an address becomes a separate element. The CSV reader with `multiLine=True` parses quoted fields and keeps a newline inside the quotes as part of the field, so one user stays one row.

### What has to be true of a file format for a line-oriented reader like `textFile()` to be safe?

Every record must fit on exactly one physical line, and a newline must never appear inside a record. Examples are CSV without embedded newlines, JSON Lines and plain log files. The file must also be splittable at any line boundary without losing context, which is what lets Spark cut it into partitions.

Note: the UUID filter only drops the continuation lines. It does not reattach them, so address fields are still unusable from this RDD.

## 4. Manual parsing of orders

### What would happen if one line in `orders.csv` had an extra comma inside `product`?

`line.split(",")` would return 6 values instead of 5, and the unpacking in `parse_order` would raise `ValueError: too many values to unpack`. Because of lazy evaluation, nothing fails when the `map` is defined. The job fails at the first action that processes that row (`take(3)` or `count()`), after the task retries.

### Where does the DataFrame API perform the equivalent check, and when?

When the data is read. The CSV reader applies a schema (declared or inferred) and handles malformed rows according to its mode: `PERMISSIVE` (default, bad fields become null), `DROPMALFORMED` or `FAILFAST`. Type errors, such as a `quantity` that is not an integer, are handled at the same stage instead of inside user code.

## 5. Key-value RDDs: total quantity per user

Both versions return the same totals. In the Spark UI (Stages tab), the `groupByKey` stage shows a larger shuffle write and shuffle read than the `reduceByKey` one.

### What does `groupByKey` shuffle instead, and why is that more expensive?

`reduceByKey` first combines the values per key inside every partition (map-side combine), so each partition sends at most one partial sum per key. `groupByKey` has no combine step: it shuffles every single `(key, value)` pair across the network and builds the full list per key on the reducer side. That means more data written and read, more serialization, and a risk of memory pressure when a key has very many values.

### For which aggregations would `groupByKey` be unavoidable?

Those that need all the values of a key at once and cannot be computed incrementally from partial results: a median or exact percentile, sorting the values of a key, collecting all values into a list. Sums, counts, min/max and averages can be expressed with `reduceByKey`, `aggregateByKey` or `combineByKey`.

## 6. Manual deduplication

`orders_dedup.count()` is lower than or equal to `orders_rdd.count()`: it is the number of distinct `order_id` values.

### What plays the role of `partitionBy()` here?

The `reduceByKey` call. Keyed by `order_id`, it shuffles all records with the same key to the same partition (hash partitioning), and the `most_recent` combiner reduces them to one. The shuffle and the grouping both come from `reduceByKey`.

### Which version states the intent more directly?

The window function: `row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC)` reads as "keep the latest row per order". The `reduceByKey` version needs a hand-written comparison, relies on the `date` strings comparing correctly (ISO format), and requires an associative and commutative combiner. It is still efficient because it combines on the map side before the shuffle.

## 7. Joining orders with users

### What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?

Both RDDs must use the same partitioner (for example `partitionBy(n)` with the same `n`) and be persisted so the partitioning is not lost. Matching keys are then already on the same partitions and the join is narrow. Without a known partitioner, `join()` shuffles both RDDs.

### What changes with `leftOuterJoin()` if a `user_id` in `orders` has no matching user?

With `join()` (inner), the order is dropped. With `leftOuterJoin()`, the order is kept and the user side is `None`: `(user_id, (order, None))`.

### What is the risk of calling `.collect()` on `enriched` for a much larger dataset?

`collect()` brings every element of every partition to the driver's single JVM. On a large dataset this can exhaust driver memory (`OutOfMemoryError`), exceed `spark.driver.maxResultSize`, and be slow. Safer options are `take(n)`, `sample`, aggregating first, or writing the result to storage.

## 8. Exercises

### 1. `aggregateByKey`

```python
agg = (orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))
       .aggregateByKey((0, 0),
                       lambda acc, q: (acc[0] + 1, acc[1] + q),
                       lambda a, b: (a[0] + b[0], a[1] + b[1])))
avg = agg.mapValues(lambda t: t[1] / t[0])
avg.take(5)
```

One pass produces `(order_count, total_quantity)` per user, and `mapValues` turns it into the average order quantity.

### 2. Fake `user_id` and `leftOuterJoin`

```python
fake = sc.parallelize([{"order_id": "fake-1", "user_id": "fake-user",
                        "date": "2026-01-01", "quantity": 1, "product": "test"}])
orders_fake = orders_rdd.union(fake)
left = orders_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
orphans = left.filter(lambda kv: kv[1][1] is None)
print(orphans.count())
```

The injected order has no matching user, so it comes back with `None` on the user side and is counted by the filter.

### 3. DataFrame version

```python
from pyspark.sql import Row
df = spark.createDataFrame(orders_rdd.map(lambda o: Row(**o)))
df.groupBy("user_id").sum("quantity").explain()
print(user_quantity.toDebugString().decode())
```

The DataFrame version is one expression instead of a `map`/`reduceByKey` chain. `explain()` shows an optimized physical plan (partial `HashAggregate`, `Exchange hashpartitioning`, final `HashAggregate`) chosen by Catalyst, while `toDebugString()` only shows the lineage exactly as written, with no optimization.

### 4. Cache timing

```python
import time
uncached = orders_data.map(parse_order)

def timed(rdd, label):
    t = time.perf_counter(); rdd.count()
    print(label, round(time.perf_counter() - t, 3), "s")

timed(orders_rdd, "cached, 1st")
timed(orders_rdd, "cached, 2nd")
timed(uncached, "uncached, 1st")
timed(uncached, "uncached, 2nd")
```

With the cache, repeated actions are faster because parsing is not redone, and the Storage tab of the Spark UI lists the cached RDD and its size in memory. Without it, the Storage tab is empty and each action re-reads and re-parses the file.
