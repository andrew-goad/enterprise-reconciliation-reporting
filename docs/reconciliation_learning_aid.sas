/*========================================================================================
 SAS Learning Aid - Plain English Guide for Your Team (CLEAN, aligned to macro v1.0)
 Audience: Analysts & developers new to SAS; teams using DATASET_RECONCILIATION_REPORT
 Version: v1.0 (this aid reflects exactly what the current macro implements)
========================================================================================

 TABLE OF CONTENTS
  0) What this guide complements
  1) SAS mental model (how SAS runs code)
  2) DATA step fundamentals (compile vs. execute; the PDV)
  3) BY-group processing, FIRST./LAST., MERGE basics
  4) Arrays, DO loops, and common idioms
  5) Formats, informats, and SAS date/time/datetime
  6) Macro language essentials (compile-time substitution)
  7) PROC SQL essentials + DICTIONARY tables
  8) TRANSPOSE (long <-> wide)
  9) Comparing values: tolerances and normalization
 10) Reconciling datasets end-to-end (how these pieces map to your macro)
 11) CSV export guardrail (simple sampling)
 12) Performance & scale tips
 13) Debugging, logging, and auditability
 14) Security & data handling (PII/PHI) - practical notes
 15) Common pitfalls cheat sheet
 16) Mini exercises (hands-on learning path)
 17) ASCII flow diagram (big picture, v1.0 features)

========================================================================================
 0) WHAT THIS GUIDE COMPLEMENTS
 ----------------------------------------------------------------------------------------

 This guide complements the reconciliation macro: DATASET_RECONCILIATION_REPORT. In v1.0 the macro:
  - Builds diagnostic outputs (Tests 1-4).
  - Consolidates a differences-only final report (ID-centric).
  - Optionally builds a WIDE (pivoted) view when VALUE_DIFF rows exist.
  - Exports CSVs with a simple row-count guard (first N rows if a cap is exceeded).
  - Reads and applies VAR_CONFIG (per-variable tolerances, normalization flags, exclusions).
  - Records run metadata (version, timestamp, inputs, options) in RECON_RUN_METADATA.
 (See the macro source for exact behavior and outputs.) [Macro source]

========================================================================================
 1) SAS MENTAL MODEL (HOW SAS RUNS CODE)
 ----------------------------------------------------------------------------------------

 - DATA steps run in two phases:
    - COMPILE: parse statements; define variable attributes (name/type/length); build Program Data Vector (PDV).
    - EXECUTE: iterate rows from input; evaluate logic; write rows with OUTPUT.
 - PROCs are specialized tools (SORT, CONTENTS, SQL, TRANSPOSE, FORMAT) that produce reports/tables.
 Tips:
 - Define LENGTH early (before assignment) to avoid unintended truncation of character variables.
 - Use KEEP/DROP on SET to reduce PDV size and memory footprint.

========================================================================================
 2) DATA STEP FUNDAMENTALS (COMPILE VS EXECUTE; THE PDV)
 ----------------------------------------------------------------------------------------

 - PDV holds the current row values while code executes.
 - LENGTH determines storage; truncation occurs silently if strings exceed length.
 - RETAIN preserves values across iterations; the SUM statement (e.g., x+1) retains implicitly.
 - OUTPUT writes the current PDV as one row; can be conditional inside IF/DO.
 Practical pattern:
  * Avoid default 8-character truncation:
    length note $200;
    set src;
    if condition then do; note = 'Flagged'; output; end;

========================================================================================
 3) BY-GROUP PROCESSING, FIRST./LAST., MERGE BASICS
 ----------------------------------------------------------------------------------------

 - BY-group processing requires inputs sorted (or indexed) by BY variables.
 - FIRST.var/LAST.var are automatic flags that mark group boundaries.
 - MERGE BY  aligns rows from multiple datasets in BY order; inputs must be sorted by &keys.
   House Rules used by the macro:
 - Before consolidating, same-named columns from A and B are kept separate during comparison logic to avoid overwrites.
 - For "keys present in one side only", MERGE two datasets limited to the keys and flag A-only/B-only.
 - Many-to-many MERGE can duplicate rows; prefer PROC SQL joins for complex relationships.
 - Optional duplicate-key diagnostics: set CHECK_KEYS_UNIQUE=YES to surface repeats per side.

========================================================================================
 4) ARRAYS, DO LOOPS, AND COMMON IDIOMS
 ----------------------------------------------------------------------------------------

 - Arrays group columns for loop logic (e.g., compare A_* vs B_* across many variables).
 - Typical pattern:
    array nA[n] var1_A var2_A ...;
    array nB[n] var1_B var2_B ...;
    length variable $32;
    do i=1 to dim(nA);
      diff = abs(nA[i] - nB[i]);
      if diff>tol then do; variable = upcase(scan(vname(nA[i]),1)); output; end;
    end;
 - vname(array[i]) returns the column name in the array; tranwrd() removes suffixes (_A/_B) if needed.

========================================================================================
 5) FORMATS, INFORMATS, AND SAS DATE/TIME/DATETIME
 ----------------------------------------------------------------------------------------

 - FORMAT controls display (not storage). Example: DATE9. prints a SAS date as 01JAN1960.
 - Informat reads raw text into values (e.g., yymmdd10.).
 - Internal representations:
    - DATE: number of days since 01JAN1960
    - TIME: number of seconds since midnight
    - DATETIME: number of seconds since 01JAN1960:00:00:00
 - Normalized formats (stripping width/decimals) reduce false positives when comparing A vs B.
 - In v1.0, the macro compares normalized format strings; there is no toggle to separately flag width.

========================================================================================
 6) MACRO LANGUAGE ESSENTIALS (COMPILE-TIME SUBSTITUTION)
 ----------------------------------------------------------------------------------------

 - Macro variables (&name) substitute text before DATA/PROC steps run.
 - %LET name=value; %PUT &=name;
 - %SYSFUNC(function()) allows many DATA step functions in macro land.
 - Quoting: %STR(), %NRSTR(), %QUOTE(), %UNQUOTE().
 - SYMGET/SYMPUTX bridge runtime DATA step and macro variables.
 Debugging (development only):
    options mprint mlogic symbolgen;  * OFF in production;

========================================================================================
 7) PROC SQL ESSENTIALS + DICTIONARY TABLES
 ----------------------------------------------------------------------------------------

 - PROC SQL excels at joins, summaries, and building macro lists via SELECT INTO.
 - DICTIONARY.COLUMNS and DICTIONARY.TABLES expose metadata (types, lengths, libraries, existence).
 - CALCULATED lets you reference derived columns in the same SELECT.
 In the macro:
  - PROC CONTENTS and SQL are used to identify common variables and to build lists for transforms.

========================================================================================
 8) TRANSPOSE (LONG <-> WIDE)
 ----------------------------------------------------------------------------------------

 - PROC TRANSPOSE pivots long rows to columns; ID becomes column names, VAR supplies values.
 - WIDE view: pivot VALUE_DIFF rows into A_ and B_ columns per key when VALUE_DIFF exists.
 - In v1.0 there is no top-N capping/ranking of WIDE columns; all differing variables are pivoted.

========================================================================================
 9) COMPARING VALUES: TOLERANCES AND NORMALIZATION
 ----------------------------------------------------------------------------------------

 - Numeric drift: absolute tolerance (NUM_TOL) ignores tiny differences (e.g., 1e-6).
 - Relative tolerance (DELTA_PCT_TOL): if both NUM_TOL and DELTA_PCT_TOL are provided, BOTH must be exceeded to flag.
 - Date/time/datetime drift: tolerances in days/seconds (DATE_TOL_DAYS, TIME_TOL_SECS, DATETIME_TOL_SECS).
 - Character normalization: per-variable TRIM/UPPER reduce cosmetic differences (whitespace/case).
 - Missing XOR: treat "present vs. missing" as a difference.
 - Type mismatch: reported in Type/Format diffs, not compared in Value diffs.
 - Note on deltas: in v1.0, deltas may be used internally for decisions but are not emitted as columns.

========================================================================================
 10) RECONCILING DATASETS END-TO-END (HOW THESE PIECES MAP TO YOUR MACRO)
 ----------------------------------------------------------------------------------------

 - Test 1 (Variables only in one): A-only / B-only schema drift (no keys).
 - Test 2 (Type/Format diffs): TYPE mismatch or normalized FORMAT mismatch.
 - Test 3 (Keys only in one): MERGE BY &keys; flags A-only vs B-only keys.
 - Test 4 (Value diffs): align A/B values via TRANSPOSE; apply tolerances/normalization; output only differences.
 - Consolidation: build a tidy final report with categories, values, notes, and a severity_rank field (no auto-sort).
 - Optional: build WIDE pivot when VALUE_DIFF exists (no top-N guard in v1.0).

========================================================================================
 11) CSV EXPORT GUARDRAIL (SIMPLE SAMPLING)
 ----------------------------------------------------------------------------------------

 - Why a guardrail: very large CSVs are impractical; the guard prevents oversized exports.
 - Logic in v1.0:
    - If EXPORT_CSV=YES and RECON_FINAL_REPORT rows > CSV_MAX_ROWS and CSV_SAMPLE_ON_GUARD=YES,
      the macro exports the first N rows instead of the full dataset (simple sample; no seed/strata).
 - Files: recon_final_report.csv, recon_final_summary_counts.csv, recon_final_summary_by_key.csv,
          recon_final_summary_by_variable.csv (the by-variable table is created during CSV export).

========================================================================================
 12) PERFORMANCE & SCALE TIPS
 ----------------------------------------------------------------------------------------

 - KEEP/DROP early; avoid carrying unused columns through merges and transposes.
 - Sort only what's needed; indexes can help if repeatedly using the same BY work.
 - Prefer PROC SQL joins for complex many-to-many relationships.
 - Use fast storage for WORK; ensure adequate space for transposes and merges.

========================================================================================
 13) DEBUGGING, LOGGING, AND AUDITABILITY
 ----------------------------------------------------------------------------------------

 - Development diagnostics: OPTIONS MPRINT MLOGIC SYMBOLGEN; %PUT macro values; PUT statements in DATA steps.
 - Audit: RECON_RUN_METADATA captures parameters (as a single options string), version, and timestamp.
 - Duplicate keys: enable CHECK_KEYS_UNIQUE=YES to surface repeats; enforce failure externally if desired.

========================================================================================
 14) SECURITY & DATA HANDLING (PII/PHI) - PRACTICAL NOTES
 ----------------------------------------------------------------------------------------

 - Avoid exporting sensitive values unless necessary; consider masking or redaction in CSVs.
 - Use secure libraries for sensitive keys; rely on access controls.
 - Keep distribution sets minimal (only summaries/IDs as needed).

========================================================================================
 15) COMMON PITFALLS CHEAT SHEET
 ----------------------------------------------------------------------------------------

 - MERGE overwrite of same-named columns -> keep A/B values separated during comparison logic.
 - Not sorted inputs for MERGE BY -> misalignment; sort first.
 - Type mismatch (numeric vs char) -> report in Type/Format diffs; do not compare values.
 - Character truncation -> define LENGTH early; note that final report values are $200 in v1.0.
 - Hidden whitespace/case -> use TRIM/UPPER per business rules via VAR_CONFIG.
 - Tolerance mis-set: too tight -> noise; too loose -> missed issues; calibrate globally then by variable.
 - Duplicate keys in inputs -> use duplicate-key diagnostics; decide warn vs fail externally.
 - Huge WIDE views -> expect all differing variables to be included (no capping in v1.0); review in SAS instead of CSV.

========================================================================================
 16) MINI EXERCISES (HANDS-ON LEARNING PATH)
 ----------------------------------------------------------------------------------------

 [Level 1]
 1) Create tiny A/B with 5 rows, 3 variables (num, char, date). Introduce:
    - One A-only variable, one B-only variable
    - One TYPE mismatch, one format width difference
    - One small numeric drift, one 1-second timestamp jitter
 2) Sort by a single key; MERGE BY key; flag A-only/B-only keys.
 3) Use VAR_CONFIG to set NUM_TOL=1e-6 for one numeric; TRIM/UPPER=YES for one char variable.
 [Level 2]
 4) Add per-variable tolerances (AMOUNT with 0.01 and 0.5% relative); exclude a noisy free-text variable.
 5) TRANSPOSE to long and compare values; confirm only differences are emitted.
 6) Request WIDE view and confirm it appears only when VALUE_DIFF exists.
 [Level 3]
 7) Enable EXPORT_CSV with a small CSV_MAX_ROWS to trigger the guardrail; verify the first-N behavior.
 8) Review RECON_RUN_METADATA to confirm version and captured options.

========================================================================================
 17) ASCII FLOW DIAGRAM (BIG PICTURE, v1.0)
 ----------------------------------------------------------------------------------------

      +-------+             +-------+
      |  dA   |             |  dB   |
      +-------+             +-------+
          |                     |
          v                     v
      PROC CONTENTS (names/types/formats) -> normalize names
          |                     |
          +----------+----------+
                     v
                Metadata join
      (common vars, exclude keys; apply VAR_CONFIG where relevant)
      		     |
      +--------------+----------------------------------------------+
      | Test 1: VARS_ONLY_IN_ONE (A_ONLY/B_ONLY)                    |
      | Test 2: TYPE_FORMAT_DIFFS                                   |
      | Test 3: KEYS_ONLY_IN_ONE (MERGE by keys)                    |
      | Test 4: VALUE_DIFFS (transpose; tolerances; TRIM/UPPER)     |
      +--------------+----------------------------------------------+
                     |
                     v
           CONSOLIDATION -> RECON_FINAL_REPORT
           (union: KEY_ONLY_IN_ONE, VALUE_DIFF, TYPE_OR_FORMAT_DIFF)
                     |
        +------------+------------+
        |                         |
        v                         v
   RECON_FINAL_WIDE_BY_KEY      Summaries (COUNTS, BY_KEY)
   (only if VALUE_DIFF)         (created with WIDE or ensured during CSV export)
                  |
                  v
            CSV Export (optional, simple guard)

========================================================================================
END OF DOCUMENT (v1.0)
========================================================================================
*/
