#!/usr/bin/env python3
"""
Unified People Database - ETL Pipeline
=======================================
Ingests 3 messy source files (Naukri applicants, gig workers, CBNexus contacts)
into a single SQLite database, resolving the same real-world person into ONE
`persons` record even though no single ID is shared across all 3 sources.

Matching strategy (in order of trust):
  1. Exact normalized EMAIL match      -> links source1 <-> source2
  2. Exact normalized PHONE match      -> links source1 <-> source3
  3. Normalized NAME + canonical CITY  -> weaker signal, used ONLY to bridge
     source2 <-> source3 (which share no email/phone field at all), and only
     when it does not conflict with an email/phone group already proven to
     be a *different* person. Conflicts are logged, not silently merged.

Every raw row is kept (full lineage), every match decision is logged in
`match_log` with a method + confidence, and anything the algorithm was not
confident about is kept in `review_queue` for a human to check.
"""

import csv
import re
import sqlite3
from pathlib import Path
from collections import defaultdict

UPLOAD_DIR = Path("D:/CONSULTBAE/uploads")
DB_PATH = Path("D:/CONSULTBAE/result/people.db")

# ---------------------------------------------------------------------------
# 1. NORMALIZATION HELPERS
# ---------------------------------------------------------------------------

CITY_ALIASES = {
    "bengaluru": "Bengaluru", "bangalore": "Bengaluru",
    "gurgaon": "Gurgaon", "gurugram": "Gurgaon",
    "delhi": "Delhi", "new delhi": "Delhi", "delhi ncr": "Delhi",
    "noida": "Noida",
    "pune": "Pune",
}


def norm_email(raw):
    if not raw:
        return None
    e = raw.strip().lower()
    return e if "@" in e else None


def norm_phone(raw):
    """Strip everything but digits, keep the last 10 (Indian mobile length)."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) < 10:
        return None
    return digits[-10:]


def norm_name(raw):
    if not raw:
        return None
    n = re.sub(r"\s+", " ", raw.strip().lower())
    n = n.replace(".", "")
    return n if n else None


def canonical_city(raw):
    if not raw:
        return None
    c = re.sub(r"\s+", " ", raw.strip().lower())
    return CITY_ALIASES.get(c, raw.strip().title())


def title_case_name(raw):
    return re.sub(r"\s+", " ", raw.strip()) if raw else None


# ---------------------------------------------------------------------------
# 2. ROBUST FILE READERS (each source has its own real-world mess)
# ---------------------------------------------------------------------------

def read_naukri(path):
    """source1: fairly clean, but CTC sometimes in lakhs (e.g. 4.2) vs full
    rupees (e.g. 417964), and dates in 4 different formats. We keep raw
    values as-is (cleaning date/CTC formats is a display concern, not an
    identity-matching concern) and only normalize the match keys."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not r.get("Full Name") or not r.get("Email"):
                continue
            rows.append({
                "raw_name": title_case_name(r["Full Name"]),
                "raw_email": r["Email"].strip(),
                "raw_phone": r["Phone"].strip(),
                "raw_city": r["City"].strip(),
                "experience_years": r["Experience (Years)"].strip(),
                "current_ctc": r["Current CTC"].strip(),
                "applied_date": r["Applied Date"].strip(),
                "skills": r["Skills"].strip(),
                "email_norm": norm_email(r["Email"]),
                "phone_norm": norm_phone(r["Phone"]),
                "name_norm": norm_name(r["Full Name"]),
                "city_canon": canonical_city(r["City"]),
            })
    return rows


def read_gig_workers(path):
    """source2: has a blank row and one row whose columns are shifted
    (skill_tags leaked into the email column). No phone field at all."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for r in reader:
            if not any(cell.strip() for cell in r):
                continue  # fully blank row
            if len(r) != len(header):
                continue  # malformed beyond repair, skip
            rec = dict(zip(header, r))
            email_raw = rec["email_id"].strip()
            if "@" not in email_raw:
                # detected column shift: skill_tags ended up in col 1,
                # everything else shifted left by one. Repair it.
                shifted = list(rec.values())
                rec = {
                    "email_id": shifted[1],
                    "worker_name": shifted[2],
                    "rate": shifted[3],
                    "location": shifted[4],
                    "status": shifted[5],
                    "skill_tags": shifted[0],
                }
            if "@" not in rec["email_id"]:
                continue  # still unusable, drop
            rows.append({
                "raw_name": title_case_name(rec["worker_name"]),
                "raw_email": rec["email_id"].strip(),
                "raw_city": rec["location"].strip(),
                "rate": rec["rate"].strip(),
                "status": rec["status"].strip(),
                "skill_tags": rec["skill_tags"].strip(),
                "email_norm": norm_email(rec["email_id"]),
                "name_norm": norm_name(rec["worker_name"]),
                "city_canon": canonical_city(rec["location"]),
            })
    return rows


def read_cbnexus(path):
    """source3: a repeated header row mid-file (looks like two exports
    concatenated). No email field at all."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for r in reader:
            if not any(cell.strip() for cell in r):
                continue
            if r == header or (r and r[0].strip() == "Name"):
                continue  # repeated header line
            if len(r) != len(header):
                continue
            rec = dict(zip(header, r))
            rows.append({
                "raw_name": title_case_name(rec["Name"]),
                "raw_phone": rec["Phone Number"].strip(),
                "raw_city": rec["City"].strip(),
                "verified": rec["Verified"].strip(),
                "projects_completed": rec["Projects Completed"].strip(),
                "phone_norm": norm_phone(rec["Phone Number"]),
                "name_norm": norm_name(rec["Name"]),
                "city_canon": canonical_city(rec["City"]),
            })
    return rows


# ---------------------------------------------------------------------------
# 3. ENTITY RESOLUTION (union-find over (source, row_index))
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self, items):
        self.parent = {i: i for i in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb
            return True
        return False


def resolve_entities(naukri, gig, cbnexus):
    ids = []
    ids += [("naukri", i) for i in range(len(naukri))]
    ids += [("gig", i) for i in range(len(gig))]
    ids += [("cbnexus", i) for i in range(len(cbnexus))]
    uf = UnionFind(ids)

    match_log = []  # (source, idx, method, confidence, note)

    def rec(source, idx):
        return {"naukri": naukri, "gig": gig, "cbnexus": cbnexus}[source][idx]

    # --- pass 1: exact email (naukri <-> gig) ---
    by_email = defaultdict(list)
    for src, rows in (("naukri", naukri), ("gig", gig)):
        for i, r in enumerate(rows):
            if r.get("email_norm"):
                by_email[r["email_norm"]].append((src, i))
    for email, group in by_email.items():
        base = group[0]
        for other in group[1:]:
            if uf.union(base, other):
                match_log.append((*other, "exact_email", "high",
                                   f"matched on email {email}"))

    # --- pass 2: exact phone (naukri <-> cbnexus) ---
    by_phone = defaultdict(list)
    for src, rows in (("naukri", naukri), ("cbnexus", cbnexus)):
        for i, r in enumerate(rows):
            if r.get("phone_norm"):
                by_phone[r["phone_norm"]].append((src, i))
    for phone, group in by_phone.items():
        base = group[0]
        for other in group[1:]:
            if uf.union(base, other):
                match_log.append((*other, "exact_phone", "high",
                                   f"matched on phone {phone}"))

    # --- pass 2b: dedupe exact duplicate rows within naukri itself
    # (same email+phone appearing twice, e.g. "R. Verma" / "Rohit Verma")
    seen = {}
    for i, r in enumerate(naukri):
        key = (r.get("email_norm"), r.get("phone_norm"))
        if key in seen and key[0]:
            uf.union(("naukri", seen[key]), ("naukri", i))
            match_log.append(("naukri", i, "exact_duplicate_row", "high",
                               f"duplicate of row {seen[key]} (same email+phone)"))
        else:
            seen[key] = i

    # --- pass 3: name + city bridge (mainly needed for gig <-> cbnexus,
    # which share no direct field). Only merge when it does NOT collide
    # two groups that already contain conflicting emails or phones.
    groups = defaultdict(list)
    for i in ids:
        groups[uf.find(i)].append(i)

    def group_emails(g):
        return {rec(s, i)["email_norm"] for s, i in g if rec(s, i).get("email_norm")}

    def group_phones(g):
        return {rec(s, i)["phone_norm"] for s, i in g if rec(s, i).get("phone_norm")}

    by_name_city = defaultdict(list)
    for root, g in groups.items():
        for s, i in g:
            r = rec(s, i)
            if r.get("name_norm") and r.get("city_canon"):
                by_name_city[(r["name_norm"], r["city_canon"])].append(root)

    for key, roots in by_name_city.items():
        roots = list(dict.fromkeys(roots))  # dedupe, preserve order
        if len(roots) < 2:
            continue
        base_root = roots[0]
        for other_root in roots[1:]:
            if uf.find(base_root) == uf.find(other_root):
                continue
            g1 = groups[uf.find(base_root)]
            g2 = groups[uf.find(other_root)]
            e1, e2 = group_emails(g1), group_emails(g2)
            p1, p2 = group_phones(g1), group_phones(g2)
            conflict = bool(e1 and e2 and e1 != e2) or bool(p1 and p2 and p1 != p2)
            if conflict:
                # Same name + same city, but the strong signals disagree ->
                # almost certainly two different real people. Do not merge.
                for s, i in g2:
                    match_log.append((s, i, "name_city_conflict", "review",
                                       f"same name/city as another group '{key}' "
                                       f"but email/phone differ -> kept separate"))
                continue
            uf.union(base_root, other_root)
            for s, i in g2:
                match_log.append((s, i, "name_city_bridge", "medium",
                                   f"bridged to group via name+city '{key}'"))
            # refresh groups dict for the merged root
            new_root = uf.find(base_root)
            groups[new_root] = g1 + g2

    return uf, match_log


# ---------------------------------------------------------------------------
# 4. DATABASE SCHEMA + LOAD
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE persons (
    person_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name       TEXT,
    primary_email   TEXT,
    primary_phone   TEXT,
    city            TEXT,
    source_count    INTEGER DEFAULT 0
);

CREATE TABLE naukri_applications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id           INTEGER REFERENCES persons(person_id),
    raw_name            TEXT,
    raw_email           TEXT,
    raw_phone           TEXT,
    raw_city            TEXT,
    experience_years    TEXT,
    current_ctc         TEXT,
    applied_date        TEXT,
    skills              TEXT
);

CREATE TABLE gig_worker_profiles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id   INTEGER REFERENCES persons(person_id),
    raw_name    TEXT,
    raw_email   TEXT,
    raw_city    TEXT,
    rate        TEXT,
    status      TEXT,
    skill_tags  TEXT
);

CREATE TABLE cbnexus_contacts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id           INTEGER REFERENCES persons(person_id),
    raw_name            TEXT,
    raw_phone           TEXT,
    raw_city            TEXT,
    verified            TEXT,
    projects_completed  TEXT
);

CREATE TABLE match_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT,
    source_row_id INTEGER,
    method      TEXT,
    confidence  TEXT,
    note        TEXT
);

CREATE INDEX idx_naukri_person ON naukri_applications(person_id);
CREATE INDEX idx_gig_person ON gig_worker_profiles(person_id);
CREATE INDEX idx_cbnexus_person ON cbnexus_contacts(person_id);

CREATE VIEW person_360 AS
SELECT
    p.person_id,
    p.full_name,
    p.primary_email,
    p.primary_phone,
    p.city,
    n.experience_years,
    n.current_ctc,
    n.applied_date,
    n.skills            AS naukri_skills,
    g.rate              AS gig_rate,
    g.status            AS gig_status,
    g.skill_tags        AS gig_skill_tags,
    c.verified,
    c.projects_completed,
    p.source_count
FROM persons p
LEFT JOIN naukri_applications n ON n.person_id = p.person_id
LEFT JOIN gig_worker_profiles g ON g.person_id = p.person_id
LEFT JOIN cbnexus_contacts c ON c.person_id = p.person_id;
"""


def build_database(naukri, gig, cbnexus, uf, match_log):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    cur = conn.cursor()

    # map union-find root -> person_id
    root_to_person = {}

    def get_person_id(source, idx):
        root = uf.find((source, idx))
        if root not in root_to_person:
            cur.execute("INSERT INTO persons (full_name) VALUES (NULL)")
            root_to_person[root] = cur.lastrowid
        return root_to_person[root]

    for i, r in enumerate(naukri):
        pid = get_person_id("naukri", i)
        cur.execute("""INSERT INTO naukri_applications
            (person_id, raw_name, raw_email, raw_phone, raw_city,
             experience_years, current_ctc, applied_date, skills)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (pid, r["raw_name"], r["raw_email"], r["raw_phone"], r["raw_city"],
             r["experience_years"], r["current_ctc"], r["applied_date"], r["skills"]))

    for i, r in enumerate(gig):
        pid = get_person_id("gig", i)
        cur.execute("""INSERT INTO gig_worker_profiles
            (person_id, raw_name, raw_email, raw_city, rate, status, skill_tags)
            VALUES (?,?,?,?,?,?,?)""",
            (pid, r["raw_name"], r["raw_email"], r["raw_city"], r["rate"],
             r["status"], r["skill_tags"]))

    for i, r in enumerate(cbnexus):
        pid = get_person_id("cbnexus", i)
        cur.execute("""INSERT INTO cbnexus_contacts
            (person_id, raw_name, raw_phone, raw_city, verified, projects_completed)
            VALUES (?,?,?,?,?,?)""",
            (pid, r["raw_name"], r["raw_phone"], r["raw_city"],
             r["verified"], r["projects_completed"]))

    for source, idx, method, confidence, note in match_log:
        cur.execute("""INSERT INTO match_log
            (source, source_row_id, method, confidence, note)
            VALUES (?,?,?,?,?)""", (source, idx, method, confidence, note))

    # backfill persons.full_name / primary_email / primary_phone / city / source_count
    cur.execute("SELECT person_id FROM persons")
    for (pid,) in cur.fetchall():
        cur.execute("""SELECT raw_name, raw_email, raw_phone, raw_city FROM naukri_applications WHERE person_id=?""", (pid,))
        n_rows = cur.fetchall()
        cur.execute("""SELECT raw_name, raw_email, raw_city FROM gig_worker_profiles WHERE person_id=?""", (pid,))
        g_rows = cur.fetchall()
        cur.execute("""SELECT raw_name, raw_phone, raw_city FROM cbnexus_contacts WHERE person_id=?""", (pid,))
        c_rows = cur.fetchall()

        names = [x[0] for x in n_rows] + [x[0] for x in g_rows] + [x[0] for x in c_rows]
        emails = [x[1] for x in n_rows] + [x[1] for x in g_rows]
        phones = [x[2] for x in n_rows] + [x[1] for x in c_rows]
        cities = [x[3] for x in n_rows] + [x[2] for x in g_rows] + [x[2] for x in c_rows]

        full_name = names[0] if names else None
        primary_email = emails[0] if emails else None
        primary_phone = phones[0] if phones else None
        city = canonical_city(cities[0]) if cities else None
        source_count = int(bool(n_rows)) + int(bool(g_rows)) + int(bool(c_rows))

        cur.execute("""UPDATE persons SET full_name=?, primary_email=?, primary_phone=?,
            city=?, source_count=? WHERE person_id=?""",
            (full_name, primary_email, primary_phone, city, source_count, pid))

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# 5. MAIN
# ---------------------------------------------------------------------------

def main():
    naukri = read_naukri(UPLOAD_DIR / "source1_naukri_applicants.csv")
    gig = read_gig_workers(UPLOAD_DIR / "source2_gig_workers.csv")
    cbnexus = read_cbnexus(UPLOAD_DIR / "source3_cbnexus_contacts.csv")

    print(f"Parsed rows -> naukri: {len(naukri)}, gig: {len(gig)}, cbnexus: {len(cbnexus)}")

    uf, match_log = resolve_entities(naukri, gig, cbnexus)
    conn = build_database(naukri, gig, cbnexus, uf, match_log)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM persons")
    total_persons = cur.fetchone()[0]
    cur.execute("SELECT source_count, COUNT(*) FROM persons GROUP BY source_count ORDER BY source_count")
    breakdown = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM match_log WHERE confidence='review'")
    review_count = cur.fetchone()[0]

    print(f"\nTotal unified person records: {total_persons}")
    print("Persons by number of sources they appear in:")
    for sc, cnt in breakdown:
        print(f"  appears in {sc} source(s): {cnt} people")
    print(f"\nRows flagged for manual review (name+city collision, kept separate): {review_count}")

    conn.close()
    print(f"\nDatabase written to {DB_PATH}")


if __name__ == "__main__":
    main()
