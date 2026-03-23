/*========================================================================================
Outputs reference for DATASET_RECONCILIATION_REPORT - v1.0
========================================================================================
PURPOSE
 This file documents every output dataset (and related CSV files) the macro produces today:
  • What each dataset contains
  • When to use it
  • Column details (schema, where relevant)
  • Key notes and edge cases
CONTEXT
  • Outputs are written to the library provided in OUTLIB (default WORK).
  • Keys and variable names are normalized (UPPER) during comparison where needed.
  • Scope is limited to functionality implemented in v1.0.

----------------------------------------------------------------------------------------
Mini “Which table should I look at?” guide

 • Quick health check → RECON_FINAL_SUMMARY_COUNTS
 • Worst records (keys) → RECON_FINAL_SUMMARY_BY_KEY
 • Noisiest variables (when CSV export ran) → RECON_FINAL_SUMMARY_BY_VARIABLE
 • Actual A vs B textual differences → RECON_FINAL_REPORT (category='VALUE_DIFF') or RECON_FINAL_WIDE_BY_KEY
 • Schema drift → RECON_VARS_ONLY_IN_ONE and RECON_TYPE_FORMAT_DIFFS
 • Missing records → RECON_KEYS_ONLY_IN_ONE
 • Duplicate keys (optional) → RECON_DUPLICATE_KEYS_A / RECON_DUPLICATE_KEYS_B

----------------------------------------------------------------------------------------
Tips & gotchas (quick reminders)

 • Keys-only presence uses MERGE; inputs are sorted by &keys (macro handles sorting).
 • TYPE/FORMAT mismatches are not reported as VALUE_DIFF-they appear in RECON_TYPE_FORMAT_DIFFS.
 • Character normalization (per-variable TRIM/UPPER via VAR_CONFIG) reduces cosmetic noise.
 • Numeric tolerance rule: if NUM_TOL and DELTA_PCT_TOL are both provided, BOTH must be exceeded to flag a diff.
 • Exclusions: with respect_exclusions_tests12=YES (default), EXCLUDE_FROM_REPORT=YES variables are removed from
   Type/Value diffs early.
 • Long values: final report/value-diff strings are stored as $200; no separate "longvals" sidecar tables in v1.0.
 • WIDE view is created only when at least one VALUE_DIFF exists; there is no top-N capping or ranking in v1.0.
 • CSV guardrail: if export_csv=YES and row count > csv_max_rows and csv_sample_on_guard=YES, the macro exports the
   first N rows (simple sample). No seeded/stratified sampling.

========================================================================================
Output overview (by stage)
========================================================================================
A. Diagnostics: variable-level (no keys)
----------------------------------------------------------------------------------------
1) OUTLIB.RECON_VARS_ONLY_IN_ONE      [TEST 1: Variables present only in one dataset]
   PURPOSE: Shows columns that exist in A only or B only (adds, drops, or renames).
   COLUMNS: name (char, UPPER), side ∈ {'A_ONLY','B_ONLY'}.
   NOTES:   This is schema-level; no key columns.

2) OUTLIB.RECON_TYPE_FORMAT_DIFFS     [TEST 2: Type/format differences on common vars]
   PURPOSE: Flags when A and B disagree on TYPE and/or FORMAT for variables present on both sides.
   COLUMNS: name (char, UPPER), typeA (1=numeric,2=char), typeB (1,2),
            fmtA_norm (char), fmtB_norm (char) - normalized format strings.
   NOTES:   No separate type_diff/format_diff booleans; width/decimals are reflected in the normalized strings.

B. Diagnostics: keys presence (A xor B)
----------------------------------------------------------------------------------------
3) OUTLIB.RECON_KEYS_ONLY_IN_ONE      [TEST 3: Keys present only in A or only in B]
   PURPOSE: Lists composite keys that exist in A-only or B-only.
   COLUMNS:  plus reporting fields consistent with the unified layout
            (category='KEY_ONLY_IN_ONE', notes='Key only in A' or 'Key only in B').
   NOTES:   Built via MERGE; other method settings fall back to MERGE with a warning.

Optional duplicate-key diagnostics (when check_keys_unique=YES)
   OUTLIB.RECON_DUPLICATE_KEYS_A / OUTLIB.RECON_DUPLICATE_KEYS_B
   PURPOSE: Key groups that repeat within A (or B).
   COLUMNS: , n (occurrence count per key group).
   NOTES:   The macro does not fail the run on duplicates; enforce failure externally if desired.

C. Diagnostics: key-level value differences (tidy long)
----------------------------------------------------------------------------------------
4) OUTLIB.RECON_VALUE_DIFFS_LONG      [TEST 4: Value differences at common keys]
   PURPOSE: Row-level list of actual differences for keys present on both sides.
   COLUMNS: , category='VALUE_DIFF', item (variable name, UPPER),
            valueA_c (char $200), valueB_c (char $200), type (numeric|char|date|time|datetime), notes.
   NOTES:   Only differences are emitted. No value-length or delta columns are written in v1.0.

D. Consolidated, ID-centric final report (only differences)
----------------------------------------------------------------------------------------
5) OUTLIB.RECON_FINAL_REPORT          [Main report to read/share - only differences]
   PURPOSE: Single, ID-centric report unifying key presence, value diffs, and type/format diffs.
   CATEGORIES: 'KEY_ONLY_IN_ONE' | 'VALUE_DIFF' | 'TYPE_OR_FORMAT_DIFF'
   COLUMNS:  (for key-level rows), category, item, valueA (char $200), valueB (char $200), type, notes,
            severity_rank (categorical rank emitted; the macro does not auto-sort by it).
   NOTES:   'VAR_ONLY_IN_ONE' is not part of the final report; see RECON_VARS_ONLY_IN_ONE for schema drift list.

5a) OUTLIB.RECON_FINAL_WIDE_BY_KEY    [Optional wide/pivoted view - created only if VALUE_DIFF exists]
    PURPOSE: One row per key with paired A_ / B_ columns for variables that differ.
    COLUMNS: , then paired columns per diffed variable.
    NOTES:   No top-N/ranking or item selection list is produced in v1.0.

E. Summaries
----------------------------------------------------------------------------------------
6) OUTLIB.RECON_FINAL_SUMMARY_COUNTS [Counts per category]
   PURPOSE: High-level counts by difference category.
   COLUMNS: category (char), n_differences (num).
   NOTES:   Created when WIDE view is built; if missing, ensured during CSV export.

7) OUTLIB.RECON_FINAL_SUMMARY_BY_KEY [Number of VALUE_DIFF rows per key]
   PURPOSE: Triage worst records by count of variable differences.
   COLUMNS: , n_value_diffs (num).
   NOTES:   Excludes rows with any missing key from the denominator.

8) OUTLIB.RECON_FINAL_SUMMARY_BY_VARIABLE [Built during CSV export]
   PURPOSE: Shows how many keys differ per variable and the diff_rate proportion.
   COLUMNS: variable (char), n_keys_differ (num), diff_rate (num).
   NOTES:   Denominator = distinct keys in the exported set; if guard sampled, denominator is the sample.

F. Governance / audit
----------------------------------------------------------------------------------------
9) OUTLIB.RECON_RUN_METADATA         [Audit trace for parameters and timestamps]
   PURPOSE: Captures macro version, timestamp, inputs, keys, and selected options.
   COLUMNS: macro_version (char), run_dttm (datetime), dA (char), dB (char), keys (char), options (char summary).

G. CSV export (when export_csv=YES)
----------------------------------------------------------------------------------------
Files written to CSV_DIR (or WORK if CSV_DIR not provided):
  • recon_final_report.csv
  • recon_final_summary_counts.csv
  • recon_final_summary_by_key.csv
  • recon_final_summary_by_variable.csv
Guard behavior:
  • If recon_final_report row count exceeds CSV_MAX_ROWS and CSV_SAMPLE_ON_GUARD=YES, the macro exports the first N
    rows instead of the full dataset. No seeded/stratified sampling.

========================================================================================
End of Outputs Reference (v1.0)
========================================================================================*/
