# Athena Resource Exhaustion — Query Analysis

A deep-dive into a workload where Athena throws **"Query resources exhausted at this scale"** on a join between the `variants` and `annotations_dedup` Iceberg (S3 Tables) tables — including diagnosis via Athena `EXPLAIN ANALYZE`, parallel reproduction on a single-node Trino setup on EC2, and an explanation of the underlying CBO mis-estimation that drives the failure.

---

## 1. Table Details

Both tables live in AWS **S3 Tables** (Iceberg, REST catalog). Schemas and partitioning summarized below.

### `variants`

| Property | Value |
|---|---|
| Total rows | **~120 B** |
| Partitioning | `contig` (identity) + `pos_start` (truncate 5,000,000) + `sample_name` (bucket 16) |
| Sort order (within partition) | `pos_start, ref, alt` |
| Bloom filters | `ref`, `alt`, `vep_gene`, `vep_symbol` |
| Target file size | 256 MB |

Key columns used in this analysis: `sample_name`, `contig`, `pos_start`, `ref`, `alt`, `genotype`, `read_depth`, `filters`, `gnomad_af`, `pathogenecity`, `data_source`.

### `annotations_dedup`

| Property | Value |
|---|---|
| Total rows | **~198 M** |
| Partitioning | `chrom` (identity) + `pos` (truncate 5,000,000) |
| Sort order (within partition) | `pos, ref, alt` |
| Bloom filters | `ref`, `alt`, `vep_gene`, `vep_symbol` |
| Target file size | 128 MB |

Key columns: `chrom`, `pos`, `ref`, `alt`, `symbol`, `pathogenecity`, `gnomad_af`, `impact`, `consequence`.

### Cardinality reference

- Distinct `(contig, pos/5M)` pairs across both tables: **612**

---

## 2. The Query Being Analysed

The carrier-count query joins variants × annotations on the canonical `(chrom, pos, ref, alt)` variant identity, applies a panel of clinical/QC predicates, then aggregates to count distinct carriers per `(gene, pathogenicity)`.

```sql
WITH a AS (
    SELECT
        symbol,
        LOWER(pathogenecity) AS pathogenecity,
        chrom, pos, ref, alt
    FROM annotations_dedup
    WHERE
    -- table-specific filters
        (gnomad_af < 0.01 OR gnomad_af IS NULL)
        AND LOWER(pathogenecity) LIKE '%pathogenic%'
    -- AND chrom = 'chr1'
    -- AND chrom = 'chr6' -- and pos between 25000000 AND 29999999
), v AS (
    SELECT
        sample_name,
        contig, pos_start, ref, alt
    FROM variants
    WHERE
    -- table-specific filters
        genotype != '0/0'
        AND read_depth >= 10
        AND filters NOT LIKE '%LowQ%'
    -- AND contig = 'chr1'
    -- AND contig = 'chr6' -- and pos_start between 25000000 AND 29999999
)
SELECT
    a.symbol AS gene,
    a.pathogenecity,
    COUNT(DISTINCT v.sample_name) AS carrier_count
FROM v INNER JOIN a
    ON v.contig = a.chrom
   AND v.pos_start = a.pos
   AND v.ref      = a.ref
   AND v.alt      = a.alt
GROUP BY a.symbol, a.pathogenecity
ORDER BY carrier_count DESC;
```

We tried **splitting this query into smaller subqueries** (chromosome- and region-scoped) so they could run in parallel against Athena. Most of the splits ran successfully — but a *specific* split (which was **smaller than several of the splits that did succeed**) consistently failed with **resource exhaustion**. That counter-intuitive result is the reason for this deeper investigation.

---

## 3. Observations for same query with different filter scopes

Same query, four different filter scopes. Smaller scopes are **not** uniformly easier — the chr6 / 25–30 Mb scope is by far the smallest workload by every cardinality measure, yet still exhausts Athena.

<table>
<thead>
<tr>
<th>Statistic</th>
<th colspan="4">Filters Applied on the Query (using <code>AND</code>)</th>
</tr>
<tr>
<th></th>
<th>table-specific filters</th>
<th>+ <code>contig=chr1</code></th>
<th>+ <code>contig=chr6</code></th>
<th>+ <code>contig=chr6</code><br>+ <code>pos=[25M–30M]</code></th>
</tr>
</thead>
<tbody>
<tr>
<td>Variants table rows</td>
<td>119 B</td>
<td>11 B</td>
<td>7 B</td>
<td>428 M</td>
</tr>
<tr>
<td>Annotations table rows</td>
<td>471 k</td>
<td>35 k</td>
<td>20 k</td>
<td>120</td>
</tr>
<tr>
<td>Distinct <code>(contig, pos/5M)</code> combos in Annotations<br>(used for dynamic filtering / partition pruning in Variants)</td>
<td>578</td>
<td>47</td>
<td>35</td>
<td>1</td>
</tr>
<tr>
<td>Distinct <code>(contig, pos, ref, alt)</code> combos for joining in Variants</td>
<td>197 M</td>
<td>18 M</td>
<td>9 M</td>
<td>609 k</td>
</tr>
<tr>
<td>Distinct <code>(contig, pos, ref, alt)</code> for joining in Annotations<br>(= annotation rows)</td>
<td>471 k</td>
<td>35 k</td>
<td>20 k</td>
<td>120</td>
</tr>
<tr>
<td>No. of rows to be joined (variants × annotations)</td>
<td>~ 119 B × 471 k</td>
<td>~ 11 B × 35 k</td>
<td>~ 7 B × 20 k</td>
<td>~ 428 M × 120</td>
</tr>
<tr>
<td><b>Query Status (with Athena)</b></td>
<td>❌ Error<br>(Query resources exhausted)</td>
<td bgcolor="#D9EAD3">✅ Success<br>Time = 50 s<br>Data scanned = 33 GB<br>Rows = 1,700</td>
<td>❌ Error<br>(Query resources exhausted)</td>
<td bgcolor="#F4CCCC">❌ Error<br>(Query resources exhausted)</td>
</tr>
<tr>
<td><b>Query Status (with Trino)</b></td>
<td>❌ Exceeding memory limit on<br>single-node setup with ~1 TB memory</td>
<td>—</td>
<td>—</td>
<td bgcolor="#D9EAD3">✅ Success<br>Time = 21 s<br>Data scanned = 2 GB<br>Rows = 26</td>
</tr>
</tbody>
</table>

### The counter-intuitive result

- The `chr1` split (11 B variant rows × 35 k annotation rows) **succeeds in Athena** in 50 s.
- The `chr6 + pos[25M–30M]` split (428 M variant rows × **120** annotation rows) — **25× smaller scan, ~290× smaller annotation side, ~30× smaller distinct-key cardinality** — **fails** in Athena with resource exhaustion.

By every measurable cardinality, the failing split is the smallest workload. That rules out scan size, join cardinality, and group-by skew as primary causes, and forces the investigation onto the **physical plan**.

### Working vs. failing query

**Working in Athena (`chr1` only):**

```sql
WITH a AS (
    SELECT symbol, LOWER(pathogenecity) AS pathogenecity, chrom, pos, ref, alt
    FROM annotations_dedup
    WHERE (gnomad_af < 0.01 OR gnomad_af IS NULL)
      AND LOWER(pathogenecity) LIKE '%pathogenic%'
      AND chrom = 'chr1'
), v AS (
    SELECT sample_name, contig, pos_start, ref, alt
    FROM variants
    WHERE genotype != '0/0'
      AND read_depth >= 10
      AND filters NOT LIKE '%LowQ%'
      AND contig = 'chr1'
)
SELECT a.symbol AS gene, a.pathogenecity,
       COUNT(DISTINCT v.sample_name) AS carrier_count
FROM v INNER JOIN a
    ON v.contig = a.chrom AND v.pos_start = a.pos
   AND v.ref = a.ref AND v.alt = a.alt
GROUP BY a.symbol, a.pathogenecity
ORDER BY carrier_count DESC;
```

**Throwing Resource Exhaustion in Athena (`chr6` + `pos in [25M, 30M]`):**

```sql
WITH a AS (
    SELECT symbol, LOWER(pathogenecity) AS pathogenecity, chrom, pos, ref, alt
    FROM annotations_dedup
    WHERE (gnomad_af < 0.01 OR gnomad_af IS NULL)
      AND LOWER(pathogenecity) LIKE '%pathogenic%'
      AND chrom = 'chr6' AND pos BETWEEN 25000000 AND 29999999
), v AS (
    SELECT sample_name, contig, pos_start, ref, alt
    FROM variants
    WHERE genotype != '0/0'
      AND read_depth >= 10
      AND filters NOT LIKE '%LowQ%'
      AND contig = 'chr6' AND pos_start BETWEEN 25000000 AND 29999999
)
SELECT a.symbol AS gene, a.pathogenecity,
       COUNT(DISTINCT v.sample_name) AS carrier_count
FROM v INNER JOIN a
    ON v.contig = a.chrom AND v.pos_start = a.pos
   AND v.ref = a.ref AND v.alt = a.alt
GROUP BY a.symbol, a.pathogenecity
ORDER BY carrier_count DESC;
```

---

## 4. Athena `EXPLAIN ANALYZE` — The Plan Mistake

### Failing splits — `contig = chr6` *and* `contig = chr6 + pos[25M–30M]`

#### Athena execution plan — chr6 (REPLICATED, wrong)

```
InnerJoin[criteria = ("pos" = "pos_start") AND ("ref_0" = "ref") AND ("alt_1" = "alt"), distribution = REPLICATED]
├─ ScanFilterProject[table = awsdatacatalog$iceberg-aws:catalog:****:s3tablescatalog/****--s3tables-variants$schema:variants.annotations_dedup]
└─ LocalExchange[partitioning = HASH, arguments = ["pos_start", "ref", "alt"]]
```

For **both** the `chr6 + pos[25M–30M]` split *and* the `chr6`-only split, Athena produces the **same plan shape**:

- The join is `distribution = REPLICATED` — i.e., a **broadcast** join (build side replicated to every worker).
- **The `annotations_dedup` table is on the probe (scan) side, with `dynamicFilters` applied** to it. Dynamic filters always flow build → probe, which means…
- **The `variants` table is the build side and is being broadcast.**

This is the opposite of what we want: variants is the *huge* table (428 M rows post-filter for the 5 Mb region, ~16 GB compressed), and annotations is the *tiny* table (120 rows / a few KB). Broadcasting variants forces every Athena worker to build a hash table over the variants slice — Athena workers have limited (and **non-customisable**) per-node memory, so this OOMs and the query fails.

Crucially: **the chr6-only split fails the same way** as chr6+pos, even though it's a much larger workload. The failure mode is plan-shape, not data-volume.

### Succeeding split — `contig = chr1`

#### Athena execution plan — chr1 (PARTITIONED)
```
InnerJoin[criteria = ("pos_start" = "pos") AND ("ref" = "ref_0") AND ("alt" = "alt_1"), distribution = PARTITIONED]
```

For the `chr1` split, Athena chose a **completely different plan**:

- The join is `distribution = PARTITIONED` — i.e., a **hash-partitioned shuffle join**, *not* a broadcast.
- Both inputs are repartitioned by the hash of the join key `(contig, pos, ref, alt)` and joined locally on each worker.
- Neither table is broadcast. There is no per-node hash-table-over-the-whole-build-side requirement, so per-node memory is bounded by the partitioned slice on each worker, not by the full filtered table.

That's why the `chr1` split succeeds despite having **25× more variant rows** than the failing chr6+pos split: the plan distributes both sides instead of putting the whole large side into every worker's memory.

### What this isolates

| Split | Athena plan | Outcome |
|---|---|---|
| `contig = chr1` | `distribution = PARTITIONED` (hash shuffle, no broadcast) | ✅ Success (50 s) |
| `contig = chr6` | `distribution = REPLICATED`, variants broadcast (annotations on probe) | ❌ Resource exhausted |
| `contig = chr6 + pos[25M–30M]` | `distribution = REPLICATED`, variants broadcast (annotations on probe) | ❌ Resource exhausted |

So: **the failing queries aren't failing because of data volume; they're failing because Athena picks `REPLICATED` with the wrong build side, while for chr1 it instead picks `PARTITIONED` and avoids the broadcast altogether.**

---

## 5. Reproducing & Diagnosing on Trino

To get more visibility into the plan and to test workarounds with `EXPLAIN ANALYZE` and session properties, we set up:

> **Single-node Trino on EC2, 1 TB memory, 192 vCPUs**, pointed at the same S3 Tables Iceberg catalog.

Trino made the **same wrong plan choice** as Athena, but the larger memory available let it complete the chr6 / 25–30 Mb query (≈25 s, peak memory 24.4 GB on the join). On Athena workers — which have a fixed, non-customisable per-node memory budget (confirmed by AWS support) — the same plan exhausts resources and aborts.

This neatly isolates the cause: **same plan on both engines, but only the larger-memory engine survives it.**

### How `SET SESSION join_distribution_type = 'BROADCAST'` changes the Trino plan

#### Trino plan **without** explicit `join_distribution_type = BROADCAST` (i.e., `AUTOMATIC` — default)

```
InnerJoin[criteria = (pos = pos_start) AND (ref_0 = ref) AND (alt_1 = alt),
         distribution = REPLICATED]
├─ ScanFilterProject[table = iceberg:variants.annotations_dedup]
└─ LocalExchange[partitioning = HASH, arguments = [pos_start::integer, ref::varchar, alt::varchar]]
```

- **`annotations_dedup` is at the scan/probe end (left).**
- **`variants` is broadcast** from the remote fragment (right / build).
- ❌ **Wrong** — large table being broadcast.

#### Trino plan **with** explicit `join_distribution_type = BROADCAST`

```
InnerJoin[criteria = (contig = chrom) AND (pos_start = pos) AND (ref = ref_0) AND (alt = alt_1),
         distribution = REPLICATED]
├─ ScanFilterProject[table = iceberg:variants.variants]
└─ LocalExchange[partitioning = HASH, arguments = [chrom::varchar, pos::integer, ref_0::varchar, alt_1::varchar]]
```

- **`variants` is at the scan/probe end (left).**
- **`annotations_dedup` is broadcast** from the remote fragment (right / build).
- ✅ **Correct** — small table being broadcast.

### How to read which side is broadcast in a Trino plan

| Signal | What it tells you |
|---|---|
| `InnerJoin[..., distribution = REPLICATED]` | This is a broadcast join (vs `PARTITIONED`). |
| Right child of a `REPLICATED` join | The **build / broadcast** side. |
| A fragment with `Output partitioning: BROADCAST []` | That fragment's output is the broadcast input — whatever scan it contains is the build side. |
| `ScanFilterProject ... dynamicFilters = {...}` | That scan is the **probe** side (DFs are *collected from* the build and *applied to* the probe). |

For a `PARTITIONED` (shuffle) join, both inputs come in via separate fragments with `Output partitioning: HASH [join_keys]` — neither side is broadcast.

---

## 6. What `join_distribution_type = BROADCAST` Actually Does

Verbatim from the [Trino documentation](https://trino.io/docs/current/admin/properties-general.html#join-distribution-type):

> **`join-distribution-type`**
>
> - **Type:** `string`
> - **Allowed values:** `AUTOMATIC`, `PARTITIONED`, `BROADCAST`
> - **Default value:** `AUTOMATIC`
> - **Session property:** `join_distribution_type`
>
> The type of distributed join to use. When set to `PARTITIONED`, Trino uses hash distributed joins. When set to `BROADCAST`, it broadcasts the right table to all nodes in the cluster that have data from the left table. Partitioned joins require redistributing both tables using a hash of the join key. This can be slower, sometimes substantially, than broadcast joins, but allows much larger joins. In particular broadcast joins are faster, if the right table is much smaller than the left. However, broadcast joins require that the tables on the right side of the join after filtering fit in memory on each node, whereas distributed joins only need to fit in distributed memory across all nodes.
>
> > **⭐ When set to `AUTOMATIC`, Trino makes a cost based decision as to which distribution type is optimal. *It considers switching the left and right inputs to the join.* In `AUTOMATIC` mode, Trino defaults to hash distributed joins if no cost could be computed, such as if the tables do not have statistics.**

The highlighted sentence is the crux of this whole investigation.

---

## 7. Why `BROADCAST` Fixes a Plan That `AUTOMATIC` Gets Wrong

When `join_distribution_type = AUTOMATIC`, Trino's CBO doesn't just choose the *distribution* — it also evaluates **swapping the join inputs** as part of the same cost comparison. With our broken row estimates:

```
variants    estimated post-filter:    865 rows  ← what the CBO believes
variants    actual    post-filter: 428,553,226 rows
annotations estimated post-filter: 82,164 rows
annotations actual    post-filter:    129 rows
```

The CBO concludes "variants is 95× smaller, so swap inputs and broadcast variants." It rewrites the plan to put variants on the right (build) and annotations on the left (probe). Wrong, but — given those estimates — internally consistent.

When `join_distribution_type = BROADCAST` is forced, the rule simplifies. Distribution is settled, and the input-swap decision falls back on a much simpler heuristic that effectively **preserves the SQL order**. Our query reads `FROM v INNER JOIN a`, so `a` (annotations) is the right operand — the natural build side in SQL semantics — and that order survives. Annotations stays on the right, becomes the build, gets broadcast.

So forcing `BROADCAST` isn't really *making the optimizer smarter about distribution*. It's **disabling the cost-based input swap that the bad statistics were misleading.**

---

## 8. The Athena vs Trino Asymmetry

| | Athena | Trino (1 TB EC2, 192 vCPUs) |
|---|---|---|
| Plan choice (chr6 / 25–30 Mb, default settings) | Variants broadcast (wrong) | Variants broadcast (wrong) |
| Per-node memory | Fixed, **non-customisable** (confirmed by AWS support) | Fully customisable |
| Outcome | ❌ Resource exhaustion | ✅ Completes (~25 s, 24.4 GB peak) |
| Session knobs (`join_distribution_type`, etc.) | ❌ Not user-exposed | ✅ Available |
| Hint syntax (`/*+ BROADCAST */`) | ❌ Not supported | ❌ Not supported (Trino doesn't support hints either) |

**Same wrong plan on both — Trino has the headroom to absorb the mistake; Athena does not.**

---

## 9. CBO Statistics on S3 Tables — A Hard Wall

Iceberg supports two layers of statistics:

1. **Manifest-derived stats** — min/max bounds, null counts, value counts. Written automatically on every data write. Drives partition pruning, range-predicate selectivity, etc.
2. **Puffin sidecar files** — extended stats including NDVs (Theta sketches). **Written only by `ANALYZE`.** Drives selectivity for equality, `IN`, `<>`, `LIKE`, etc.

In our environment:

```
S3 Tables do not support analyze   -- error returned by Trino
```

Both `ANALYZE` and `SHOW STATS FOR <table>` are rejected by the AWS-managed S3 Tables REST catalog. The metadata-extension surface is locked down. The optimizer is therefore *permanently* limited to manifest stats (no NDVs), and any selectivity mis-estimates that arise from missing NDV stats **cannot** be fixed by feeding the optimizer better statistics.

> **Consequence:** at this scale, manual customisation and optimisation cannot be fully avoided, and that comes with at least some level of ongoing maintenance.

---

## 10. Whole-Genome Plan — Timing Analysis (Trino, with `BROADCAST` forced)

For the whole-genome version of the query (no chrom/pos filter), Trino with `join_distribution_type=BROADCAST` runs the plan in the right shape and completes in **4.11 minutes wall clock**.

### Wall clock vs CPU

```
Wall clock:        4.11 min  (Execution)
Total CPU:        ~10.5 hours  (Fragment 5 alone: 10.36 h)
Total scheduled:  ~1.02 days   (Fragment 5 alone)
```

CPU/wall ≈ **150× parallelism** — the cluster is being used effectively. Wall clock isn't a parallelism problem.

### Time per fragment

| Fragment | CPU | Scheduled | Blocked | Notes |
|---|---|---|---|---|
| 6 (annotations scan) | 15.40 s | 1.09 m | 0 | trivial, broadcast |
| **5 (variants scan + join)** | **10.36 h** | **1.02 d** | 2.42 h | **the cost center** |
| 4 (final aggregate) | 8.08 m | 9.98 m | 1.83 h | 38 GB peak memory |
| 3 (partial → final agg) | 220 ms | 240 ms | 4.38 h | mostly waiting |
| 2 (sort) | 89 ms | 90 ms | 2.26 h | mostly waiting |
| 1 (output) | 2 ms | 2 ms | 4.11 m | output streaming |

Upstream "Blocked" hours are the downstream fragments waiting on fragment 5 — confirmation that the bottleneck is the variants scan.

### Inside Fragment 5 — the 4-minute wall clock

```
Physical input time:    2.57 h    ← aggregate across all tasks reading 365.79 GB
ScanFilterProject CPU:  6.33 h    ← filter eval on 120 B rows
InnerJoin CPU:          3.83 h    ← probe 119 B rows against ~471 k build
PARTIAL aggregate CPU:  11.23 m   ← collapsing 1.45 B join output → 491 M rows
Total fragment CPU:    10.36 h
```

**Two-thirds of CPU is scan + predicate evaluation on variants.** The join itself is only 37% of CPU, and the partial aggregate is 1.8%. The bottleneck is reading and filtering 12.71 TB of variants — not the join, not the aggregate.

### What the timings rule out

- **Not network/shuffle bound.** Fragment 4 receives 28.56 GB across the cluster with CPU 8 m vs scheduled 10 m — almost no waiting.
- **Not aggregate-bound.** 491 M intermediate rows at 38 GB peak completes in 8 m CPU. Trivial next to the scan.
- **Not metadata-bound.** Splits generation: 957 ms for 22,068 splits.
- **Not DF-collection bound.** DFs collected in 471 ms — instant.

> **Verdict:** scan-bound, parallelism-saturated, plan is fine. To go faster, materialize.

---

## 11. Whole-Genome Plan — Row Count Analysis

Tracing row counts top-to-bottom shows where data actually compresses (and where it doesn't).

### Fragment 5 — input through join output

| Stage | Rows | Bytes | Reduction vs prior |
|---|---|---|---|
| variants raw read | 120,089,370,515 | 12.71 TB | — |
| after `genotype` / `read_depth` / `filters` predicates | 119,076,006,119 | 9.82 TB | **0.84% filtered** |
| after dynamic filter join on `(contig, pos, ref, alt)` | (same as above) | (same) | DF was inert |
| **InnerJoin output** | 1,455,279,868 | 86.03 GB | 82× from 119 B |
| PARTIAL aggregate by `(symbol, expr, sample_name)` | 491,790,436 | 28.56 GB | 3× from 1.45 B |

**Things that jump out:**

- **The predicate filter is barely doing anything.** 119/120 B rows survive. The CBO estimated 120 B → 1.28 M (off by **~93,000×**). Reality: 0.84% filtered.
- **DF collection happened (471 ms), but the DF ranges were genome-wide:**
  ```
  df_792 (contig):  [chr1, chrY]                ← spans entire genome
  df_793 (pos):     [45695, 247921109]          ← spans 250 Mb
  df_794 (ref/alt): [-, TTTTTTT...]             ← effectively unbounded
  ```
  Because annotations after filter span the whole genome, the broadcast-side min/max ranges are too wide to prune anything. **DFs only help when the build side is narrow on the join keys.** With a `chrom` or `pos` filter on annotations, DFs become powerful (in the chr1 plan, DF had 31,998 ranges and pruned aggressively); without one, they're inert. So the 12.71 TB of physical I/O really did get touched.
- **The join is the real reducer:** 119 B → 1.45 B (82× reduction). That's the inner-join filtering on `(contig, pos, ref, alt)` against the 471 k annotation keys. Variants whose join key isn't in annotations get dropped. This is the actual selectivity that the CBO should have predicted *via the join* but couldn't predict via the predicates alone.

### Fragment 6 — annotations build side

| Stage | Rows | Bytes |
|---|---|---|
| annotations raw read | 197,816,827 | 10.61 GB |
| after `gnomad_af` and `LIKE '%pathogenic%'` | 471,246 | 42.96 MB |

**99.76% filtered** — a 250× reduction. The CBO estimated 38 M post-filter (off by 80×) but the real answer is small enough that broadcasting is trivial regardless.

### Fragment 4 — cross-cluster aggregate

| Stage | Rows | Bytes |
|---|---|---|
| input (after HASH redistribute on `symbol/expr`) | 491,790,436 | 28.56 GB |
| FINAL distinct on `(symbol, expr, sample_name)` | 487,662,321 | 28.32 GB |
| PARTIAL count by `(symbol, expr)` | 587,936 | 27.44 MB |

The first step (`Aggregate FINAL keys = [symbol, expr, sample_name]`) only deduplicates 0.84% — meaning the 491 M rows arrived almost-unique because the partial agg in fragment 5 happened pre-shuffle on the same key. Across-worker duplicates are rare.

The huge collapse — **491 M → 588 k**, 836× — happens at the next step where `count(DISTINCT sample_name)` becomes `count(*)` per `(symbol, expr)` group. That's the real funnel.

### Fragments 3 → 2 → 1

| Fragment | Rows | Bytes |
|---|---|---|
| 3 (FINAL count by `symbol/expr`) | 587,936 → 18,373 | 27.44 MB → 878 kB |
| 2 (sort) | 18,373 | 878 kB |
| 1 (output) | 18,373 | 878 kB |

The 587,936 → 18,373 collapse in fragment 3 is a **32× reduction** during FINAL aggregate — each `(symbol, expr)` group had ~32 partial entries, consistent with the cluster's ~150× parallelism budget split across the aggregate.

### Cardinality summary

```
120,089,370,515   variants raw input
119,076,006,119   after predicates              (0.84% reduction)
  1,455,279,868   after join (inner on 471 k)   (82× reduction)
    491,790,436   after partial dedup           (3× reduction)
    487,662,321   after global dedup            (1× — almost no dups)
        587,936   after partial count agg       (836× reduction)
         18,373   after final count agg         (32× reduction)
         18,373   final output rows
```

### What the row counts argue

- **The predicate filter is dead weight.** It's not selective and the CBO doesn't know that — its post-filter row estimate is ~93,000× too low. Predicate rewriting (replacing `<>`/wildcard `LIKE`/function-wrapped column forms with shapes the CBO can estimate, like explicit `IN (...)` lists) is worth trying as a way to nudge the row estimates closer to reality, but it isn't a guaranteed fix and won't change wall clock — the workload is still scan-bound either way.
- **The join is doing the real filtering.** 471 k annotation keys filter 119 B variants → 1.45 B. The most useful materialization candidate is the *filtered annotation key set* (`SELECT DISTINCT chrom, pos, ref, alt FROM annotations_dedup WHERE … pathogenic …`) — already 471 k rows / 43 MB.
- **Sample distinct-set is the second reducer.** 1.45 B join rows have 487 M distinct `(symbol, pathogenicity, sample_name)` triples — each variant carrier produces ~3 rows on average; dedup buys ~3×. The big collapse comes at the per-gene count step (836×).
- **Final answer is 18 k rows.** The whole pipeline exists to compress 120 B → 18 k. The compression is dominated by the join (82×) and the count (836×). Everything else is plumbing.

---

## 12. Memory Pressure — Where the Real Watermark Is

> Memory pressure in this workload is in the **final aggregate, not the join**.
>
> Peak: fragment 4 = **38.01 GB**, fragment 5 = 10.38 GB.
>
> The 38 GB is the FINAL aggregate hashing `(symbol, expr, sample_name)` over **487 M intermediate rows**. **This is the real watermark for sizing the cluster.**

Athena workers have a fixed per-node memory budget that cannot accommodate this aggregate at whole-genome scope. Trino on a 1 TB box can.

---

## 13. Workarounds Available, by Engine

### Trino (local, EC2)

In priority order:

1. **`SET SESSION join_distribution_type = 'BROADCAST'`** — what we're using; works but is a blunt instrument (forces broadcast for every join in the session & but also preserves the SQL order for the join, for deciding which table to use at probe end (left) & which to use at build/broadcast end (right)).
2. **`SET SESSION join_max_broadcast_table_size = '50MB'`** while keeping `AUTOMATIC` distribution. Caps which sides are eligible for broadcasting. Annotations (~43 MB post-filter) stays eligible; variants (16 GB) doesn't. Even with bad stats, the optimizer can't pick variants as build because it's above the threshold. **Safer than forcing `BROADCAST` globally.**
3. **`SET SESSION join_reordering_strategy = 'NONE'`** — preserves SQL order, so writing `FROM variants v INNER JOIN annotations_dedup a` survives the optimizer.

### Athena

None of the above session knobs are user-exposed. No comment-style hints (`/*+ BROADCAST */`) are honored. Available paths:

1. **Predicate rewriting** — replace forms the CBO cannot estimate well (e.g. `<>`, wildcard `LIKE`, function-wrapped columns) with equivalent forms that have clearer selectivity (e.g. explicit `IN (...)` lists). Aim is to give the optimizer row estimates close enough to reality that it stops choosing `REPLICATED` with the wrong build side.
2. **CTAS-then-join** — materialize the small filtered annotations side as a temp Iceberg table first, then join. The temp table has accurate row-count metadata, so the optimizer no longer has to guess.

---

## 14. Conclusions

1. **The failure is a planner mistake, not a data-volume problem.** The smallest workload by every cardinality measure is the one that fails — because Athena selects `distribution = REPLICATED` with the wrong build side (variants), and Athena workers with fixed per-node memory can't absorb the resulting broadcast.

2. **The chr1 split survives because Athena chose a different distribution entirely** — `PARTITIONED` (hash shuffle) instead of `REPLICATED`. Neither side is broadcast, so per-node memory is bounded by the partitioned slice, not the full filtered table. The chr6 and chr6+pos splits get `REPLICATED` and fail.

3. **Statistics extensions (`ANALYZE`, Puffin files, `SHOW STATS`) are blocked on S3 Tables.** The CBO cannot be repaired by feeding it better statistics — only by reshaping the query or forcing the plan.

4. **`SET SESSION join_distribution_type = 'BROADCAST'` works on Trino because it disables the cost-based input swap**, not because it changes distribution semantics. Default `AUTOMATIC` mode considers swapping the left and right inputs as part of the cost comparison — and that's the rule that gets confused on this workload.

5. **Trino on a 1 TB EC2 box succeeds where Athena fails** purely because of memory headroom — same wrong plan, but enough RAM to broadcast a 16 GB variants slice. AWS support has confirmed Athena per-node memory is **not customisable**.

6. **At this scale, manual customisation and optimisation cannot be fully avoided** — and that comes with at least some level of ongoing maintenance overhead.

---
