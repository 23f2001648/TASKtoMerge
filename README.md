# Unified People Database

`people.db` (SQLite) merges the 3 messy source files into one clean, deduplicated
database of people. `build_db.py` is the pipeline that produced it — rerun it
any time with `python3 build_db.py` (reads from `/mnt/user-data/uploads`).

## The matching problem

No column is common to all 3 files:

| Source | Has Email | Has Phone | Has Name/City |
|---|---|---|---|
| source1 (Naukri applicants) | ✅ | ✅ | ✅ |
| source2 (gig workers) | ✅ | ❌ | ✅ |
| source3 (CBNexus contacts) | ❌ | ✅ | ✅ |

So identity has to be inferred via a chain: **email** bridges source1↔source2,
**phone** bridges source1↔source3, and for anyone who only appears in
source2+source3 (no source1 record to bridge through), the only option left is
**normalized name + canonical city**.

## Matching logic (in `resolve_entities`, trust-ordered)

1. **Exact email match** (case/whitespace normalized) — high confidence.
2. **Exact phone match** (digits only, last 10 kept, so `+91-9000000131`,
   `09000000131`, and `9000000131` all match) — high confidence.
3. **Exact duplicate row** within a source (e.g. "Rohit Verma" and "R. Verma"
   with identical email+phone) — deduped as the same row.
4. **Name + city bridge** — only used to connect groups that have *no*
   email/phone overlap yet. Cities are aliased first (Bangalore=Bengaluru,
   Gurgaon=Gurugram, Delhi/New Delhi/Delhi NCR=Delhi) so "same city" isn't
   defeated by spelling.

**Conflict safety net:** before step 4 merges two groups, it checks whether
each group already has its *own* verified email/phone. If group A is
`arjun.mehta9@... / 09000000131` and group B is a *different* phone
`9000000272` with the same name+city, they are **not** merged — that's two
real "Arjun Mehta"s in Noida, not one person with messy data. These cases are
written to `match_log` with `confidence='review'` instead of being guessed
at silently.

## Schema

- `persons` — one row per real person. `full_name`, `primary_email`,
  `primary_phone`, `city` are best-available picks; `source_count` tells you
  how many of the 3 files mention them (1, 2, or 3).
- `naukri_applications`, `gig_worker_profiles`, `cbnexus_contacts` — the raw
  data from each source, one row per original CSV row, each linked to a
  `person_id`. Nothing is thrown away — full lineage is kept.
- `match_log` — every match decision made (which rows got merged, by what
  method, at what confidence), plus the 2 name/city collisions that were
  deliberately kept separate for human review.
- `person_360` — a convenience view joining everything into one row per
  person for querying.

## Data-cleaning notes baked into the pipeline

- source2 had a blank row and one row with shifted columns (skill_tags had
  leaked into the email column) — detected and repaired/dropped.
- source3 had a repeated header row midway through the file (looks like two
  exports concatenated) — filtered out.
- Phone numbers arrive in at least 4 formats (`+91...`, `91...`, `0...`,
  bare 10-digit) — all normalized to the last 10 digits for matching, while
  the original raw value is preserved untouched in the detail tables.

## Result

- 42 Naukri rows + 31 gig-worker rows + 30 CBNexus rows → **56 unique people**
- 15 people appear in all 3 sources, 14 in exactly 2, 27 in only 1
- 2 rows flagged in `match_log` as a same-name/city collision that were
  correctly kept as separate people
