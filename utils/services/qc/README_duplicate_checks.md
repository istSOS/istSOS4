# qc: avoiding duplicate checks

**Superseded.** `qc` now finds new observations by `resultTime` (the arrival
time, set again when a result is edited) instead of `systemTimeValidity`. QC's
own PATCHes don't change `resultTime`, so duplicate A no longer happens. Only
the overlap (B) is still checked twice, and that second check writes nothing.
See [README.md](README.md). This file is the record of the options considered
for `systemTimeValidity`.

## Context

On each run, `qc` picks up new observations with
`lower(systemTimeValidity) gt w - 60 s`, where `w` is the service's clock at
the previous run. Observations come back a second time for two reasons:

- **A. QC's own writes.** A `resultQuality` PATCH creates a new version, so the
  next run sees the rows it just patched.
- **B. The 60 s overlap.** The overlap exists because `current_timestamp` is
  the time the transaction started, so rows committed late can carry an older
  timestamp.

**Cost of doing nothing:** about one extra interval re-checked per run, with
**zero writes**. The quality comes out the same, so there's no PATCH and no
new version. The only bigger case is the run after the first full run, which
re-checks the whole datastream once. For scale, every run already reads N-1
buffer rows.

## Requirement for any option: the operator-edit race

1. 15:00:05 — QC reads `result=12.3`.
2. 15:00:20 — an operator corrects it to `50`.
3. 15:00:37 — QC writes the quality it computed from 12.3.

Today, QC's write is a new version, so the next run re-checks the row and
fixes the quality. An option that hides QC's writes must not lose this.

## Options

### 0. Accept it
- No change.
- Safe in the race.
- Cost as above. The logs show "N checked, 0 updated".

### 1. Fingerprint QC's writes, in memory
Keep `id -> (phenomenonTime, result, resultQuality written)` for each row
patched in the last run. Next run, skip the new rows that still match.
- Removes A, and B except overlap rows QC didn't patch.
- Safe in the race: an edited `result` no longer matches.
- No core change. `check_datastream` has to return what it wrote.
- Memory: the rows written in the last run, which is the whole datastream
  once, after the first run. A restart means one run with duplicates.

### 2. Filter out QC's commits (`Commit/author ne '<qc user>'`)
- Removes A, and B except overlap rows QC didn't patch.
- **Not safe in the race:** the current version is QC's, so the row is
  skipped and its quality stays wrong for good.
- Untested filter through Commit, which adds a join on Observation.
- Also skips Flink's writes if Flink uses the same user.

### 3. Trigger: don't version updates that change only `resultQuality`
The trigger already does this for some Datastream and Location columns.
- Removes A. B remains.
- Safe in the race: the operator's version keeps its `from`.
- **Policy change:** quality changes are no longer in history, and `as_of`
  shows today's quality for past instants. This affects Flink QC too.
- Core change: a new database image and a migration.

### 4. Backdate QC's `systemTimeValidity` to before `w - 60 s`
- The range `[old_from, X)` needs X ≥ `old_from`. Overlap rows can't be
  backdated, so they need a clamp and stay duplicated.
- Safe in the race with the clamp, but the operator's version shrinks to an
  empty range.
- **False `as_of`:** flags show up before they were written, which breaks
  reproducing an export.
- Core change in the API, the trigger, and the role checks, so that nobody
  else can rewrite history.
- Taken to its end, this is option 3 with more parts.

### 5. Event-driven (`NOTIFY` or Kafka)
- No duplicates.
- Rebuilds the Flink path, and needs direct database access or Kafka.

## Comparison

| Option | A | B | Race | History stays true | Core change |
|---|---|---|---|---|---|
| 0. Accept | dup | dup | safe | yes | none |
| 1. Fingerprint | removed | mostly removed | safe | yes | none |
| 2. Commit author | removed | mostly removed | **unsafe** | yes | none |
| 3. No version for quality | removed | dup | safe | no, for quality | trigger |
| 4. Backdate | removed, except overlap | dup | safe | **no** | API, trigger, role |
| 5. Event-driven | removed | removed | safe | yes | DB access or Kafka |

## Recommendation

Start with **0**, and move to **1** if the duplicate work or the log noise
matters. Option **3** only if the team decides quality changes need no history.
Avoid **2** and **4**.
