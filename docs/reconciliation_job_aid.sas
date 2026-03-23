/*=========================================================================================================
 JOB AID / RUN PROCEDURE -- DATASET_RECONCILIATION_REPORT (v1.0)
===========================================================================================================
 Purpose
  This guide is an operational runbook for the v1.0 macro that reconciles two SAS datasets (A vs B) by
  keys, detects schema and value differences with tolerances, and produces an analyst-friendly final report.
 
 Versioning & Governance
  - Macro version recorded in: OUTLIB.RECON_RUN_METADATA (macro_version='v1.0', run timestamp, inputs, options).
  - Keep a team changelog for both the macro version used and your VAR_CONFIG revisions.
===========================================================================================================
 PHASE 1 -- PREP
===========================================================================================================

 [ ] Confirm Environment:
      - SAS 9.4 available; 
      - sufficient WORK space. 
      - Macro v1.0 is loaded in the EG session (no %INCLUDE in UAT). 
 [ ] Identify Inputs:
      - dA and dB ready (LIBREF.MEMBER). 
      - keys finalized (e.g., LOAN_ID CUSTOMER_ID). 
      - Optional VAR_CONFIG ready (per-variable tolerances, normalization flags, exclusions). 
 [ ] Set Output:
      - Choose OUTLIB (e.g., WORK or a project library). 
 [ ] Sanity Checks:
      - datasets exist; keys present in both; 
      - decide whether to enable duplicate-key detection.

===========================================================================================================
 PHASE 2 -- RUN (Parameters that are ACTUALLY supported today)
===========================================================================================================*/

/* A) Minimal Execution (defaults) */
%DATASET_RECONCILIATION_REPORT(
  dA=libA.tableA,
  dB=libB.tableB,
  keys=loan_id customer_id,
  outlib=work
);
/* Outputs all SAS tables in OUTLIB; no CSVs; adds a final report and (when add_wide_view=YES) summaries. */ 

/* B) Standard Governance Run (recommended) */
%DATASET_RECONCILIATION_REPORT(
  dA=libA.tableA,
  dB=libB.tableB,
  keys=loan_id customer_id,
  outlib=work,
  var_config=work.var_config,       /* per-variable overrides: tolerances, trim/upper, exclusions */
  check_keys_unique=YES,            /* writes RECON_DUPLICATE_KEYS_A/B if duplicates exist */
  respect_exclusions_tests12=YES,   /* removes excluded vars from Type/Value diffs (Tests 1&2) */
  add_numeric_deltas=YES,           /* used internally for decisions; deltas are NOT emitted as columns */ 
  add_wide_view=YES                 /* builds WIDE view only if VALUE_DIFF rows exist */ 
);

/* Notes:
    - keys_only_method=MERGE is the only implemented engine; other values fallback to MERGE with a warning.
    - longvals_enable/value_len_max exist as parameters but no dedicated "longvals" sidecars are produced.
    - Final human-readable values in reports use fixed $200 length.
*/ 

/* C) CSV Export (guarded and simple) */
%DATASET_RECONCILIATION_REPORT(
  dA=libA.tableA,
  dB=libB.tableB,
  keys=loan_id customer_id,
  outlib=work,
  export_csv=YES,
  csv_dir='/secure/server/path',      /* if omitted, defaults to WORK path */
  csv_auto_guard=YES,                /* if rows > csv_max_rows, export a SAMPLE instead of full */
  csv_max_rows=1000000,
  csv_sample_on_guard=YES,           /* SAMPLE = first N rows; no stratified/seeded sampling */
  csv_sample_include_typefmt=YES
);
/* When CSVs are requested, the macro ensures summary tables exist and also builds a by-variable summary (see Review). */ 

/*=========================================================================================================
 WHAT THE MACRO DOES (CONCEPTUAL FLOW)
===========================================================================================================

 1) Preflight & Metadata
    - Validates inputs and keys; records run metadata to RECON_RUN_METADATA.
    - Captures variable name/type/format from each side via PROC CONTENTS.

 2) Test
    Test 1: Variables only in one side -> OUTLIB.RECON_VARS_ONLY_IN_ONE
    Test 2: Type/Format differences for common vars -> OUTLIB.RECON_TYPE_FORMAT_DIFFS
    Test 3: Keys-only presence (A xor B) via MERGE -> OUTLIB.RECON_KEYS_ONLY_IN_ONE
    Test 4: Value differences (numeric + char) with tolerances/normalization -> OUTLIB.RECON_VALUE_DIFFS_LONG

 3) Final Report & Optional WIDE
    - OUTLIB.RECON_FINAL_REPORT = union of KEY_ONLY_IN_ONE, VALUE_DIFF, TYPE_OR_FORMAT_DIFF rows, 
      plus a severity_rank column (sorting is not automatic). 
    - If add_wide_view=YES and at least one VALUE_DIFF exists, builds OUTLIB.RECON_FINAL_WIDE_BY_KEY by pivoting VALUE_DIFF values; 
      no top-N capping/ranking is applied in v1.0. 

 4) Summaries
    - If add_wide_view=YES, creates: 
        OUTLIB.RECON_FINAL_SUMMARY_COUNTS (counts by category), and 
        OUTLIB.RECON_FINAL_SUMMARY_BY_KEY (excludes rows with any missing key). 
    - If add_wide_view=NO, these are created only when export_csv=YES (CSV section ensures they exist). 

 5) CSV Export (when requested)
    - Writes recon_final_report.csv and summaries to csv_dir (defaults to WORK if blank). 
    - If row count exceeds csv_max_rows and csv_sample_on_guard=YES, exports first N rows as SAMPLE. 
    - Generates OUTLIB.RECON_FINAL_SUMMARY_BY_VARIABLE during CSV export and writes its CSV; 
      diff_rate's denominator equals distinct keyed rows in the exported (possibly sampled) set. 

===========================================================================================================
 PHASE 3 -- REVIEW (Outputs you will actually see)
===========================================================================================================

 Health & Coverage
  [ ] RECON_FINAL_SUMMARY_COUNTS (if created per rules above) 
  [ ] RECON_RUN_METADATA (version, timestamp, key list, selected options) 

 Keys & Duplicates
  [ ] RECON_KEYS_ONLY_IN_ONE: magnitude of A-only vs B-only keys (MERGE-based). 
  [ ] (If enabled) RECON_DUPLICATE_KEYS_A / RECON_DUPLICATE_KEYS_B for governance; macro does not "fail-fast". 
      Enforce failure externally if required. 

 Schema Drift
  [ ] RECON_VARS_ONLY_IN_ONE: added/dropped variables between A and B. 
  [ ] RECON_TYPE_FORMAT_DIFFS: TYPE/FORMAT mismatches (format width/decimals normalized). 

 Value Differences (core)
  [ ] RECON_FINAL_REPORT where category='VALUE_DIFF' for spot checks. 
  [ ] (If CSV export on) RECON_FINAL_SUMMARY_BY_VARIABLE: noisiest variables; note denominator caveat if sampling triggered. 
  [ ] RECON_FINAL_SUMMARY_BY_KEY: worst records; excludes any rows with missing keys in the denominator. 
  [ ] RECON_FINAL_WIDE_BY_KEY (if present): side-by-side values per key; includes all VALUE_DIFF variables (no top-N). 

 Interpreting Common Patterns (practical)
  - NUMERIC: If both absolute num_tol and relative delta_pct_tol are provided, BOTH must be exceeded to flag a diff.
  - DATE/TIME/DATETIME classes compare underlying numeric values against respective tolerances.
  - CHAR: per-variable TRIM/UPPER normalization reduces cosmetic differences.

===========================================================================================================
 PHASE 4 -- PACKAGE & HANDOFF
===========================================================================================================

 Minimum package to stakeholders
  [ ] RECON_FINAL_REPORT (SAS dataset and/or CSV) 
  [ ] RECON_FINAL_SUMMARY_COUNTS (if created) 
  [ ] RECON_FINAL_SUMMARY_BY_KEY (if created) and (if CSV export on) RECON_FINAL_SUMMARY_BY_VARIABLE 
  [ ] RECON_FINAL_WIDE_BY_KEY (if present) 
  [ ] RECON_RUN_METADATA (audit) 

 CSV notes to include in your handoff
  - If the guard sampled the final report, call that out and attach parameters (csv_auto_guard, csv_max_rows). 
  - By-variable diff_rate is computed over the exported (sampled or full) denominator. 

===========================================================================================================
 VAR_CONFIG -- What columns are recognized today
===========================================================================================================

 Columns recognized (case-insensitive on names; others are ignored):
  VAR_NAME
  CLASS_OVERRIDE        (NUMERIC | CHAR | DATE | TIME | DATETIME)
  NUM_TOL
  DATE_TOL_DAYS
  TIME_TOL_SECS
  DATETIME_TOL_SECS
  DELTA_PCT_TOL         (0-1; if provided alongside NUM_TOL, both must be exceeded)
  CHAR_TRIM             (YES/NO)
  CHAR_UPPER            (YES/NO)
  EXCLUDE_FROM_REPORT   (YES/NO)
  COMMENT               (optional; ignored by logic, useful for audit docs)

 Operational tips:
  - Use CLASS_OVERRIDE for timestamps stored as numbers, etc.
  - Use EXCLUDE_FROM_REPORT=YES for noisy variables to hide them from Type/Value diffs in the final report.
  - Keep a short COMMENT for audit traceability.

-----------------------------------------------------------------------------------------------------------
VAR_CONFIG -- Example A (SAS DATA step to build work.var_config)
-----------------------------------------------------------------------------------------------------------*/

/* Example VAR_CONFIG for common A/B dataset variables */
/* Keep this in WORK or your project library before running the macro */

data work.var_config; 
length 
    var_name              $32 
    class_override        $10 
    char_trim             $3 
    char_upper            $3 
    exclude_from_report   $3 
    comment               $200 
 ;
format num_tol best32. 
        date_tol_days best32. 
        time_tol_secs best32. 
        datetime_tol_secs best32. 
        delta_pct_tol best32.; 
/* --------- NUMERIC EXAMPLES --------- */ 

 /* Money fields: allow 1 cent absolute AND small relative noise-both must be exceeded to flag */ 
 var_name='AMOUNT'; 
 class_override='NUMERIC'; num_tol=0.01; delta_pct_tol=0.005; 
 date_tol_days=.; time_tol_secs=.; datetime_tol_secs=.; char_trim=''; char_upper=''; 
 exclude_from_report='NO'; comment='Money fields permit ±$0.01 and 0.5% relative-both must be exceeded'; output; 
/* Rates: tight absolute tolerance only */ 
 var_name='RATE';           class_override='NUMERIC'; num_tol=1e-6; delta_pct_tol=.; 
 date_tol_days=.; time_tol_secs=.; datetime_tol_secs=.; char_trim=''; char_upper=''; 
 exclude_from_report='NO'; 
 comment='Tiny absolute differences only'; output; 

 /* Count-like metrics: small absolute jitter */ 
 var_name='NUM_TRANSACTIONS'; class_override='NUMERIC'; num_tol=1;  delta_pct_tol=.; 
 date_tol_days=.; time_tol_secs=.; datetime_tol_secs=.; char_trim=''; 
 char_upper=''; 
 exclude_from_report='NO'; comment='Off-by-1 tolerated for counters'; output; 

 /* --------- DATE/TIME/DATETIME EXAMPLES --------- */ 

 /* Close dates: allow ±1 day window */ 
 var_name='CLOSE_DATE'; 
 class_override='DATE';      date_tol_days=1; 
 num_tol=.; time_tol_secs=.; datetime_tol_secs=.; delta_pct_tol=.; char_trim=''; char_upper=''; 
 exclude_from_report='NO'; comment='Date window of 1 day'; output; 
/* ETL cutover time: allow ±2 seconds */ 
 var_name='CUTOVER_TIME';    class_override='TIME';      time_tol_secs=2; 
 num_tol=.; date_tol_days=.; datetime_tol_secs=.; delta_pct_tol=.; char_trim=''; char_upper=''; 
 exclude_from_report='NO'; 
 comment='Time window of 2 seconds'; output; 

 /* Update timestamp stored as seconds since epoch: treat as DATETIME with ±2s */ 
 var_name='UPDATE_TS'; 
 class_override='DATETIME'; datetime_tol_secs=2; 
 num_tol=.; date_tol_days=.; time_tol_secs=.; delta_pct_tol=.; char_trim=''; char_upper=''; 
 exclude_from_report='NO'; comment='Datetime window of 2 seconds'; output; 
/* --------- CHARACTER EXAMPLES --------- */ 

 /* Names: trim whitespace and compare case-insensitively */ 
 var_name='CUSTOMER_NAME';   class_override='CHAR'; char_trim='YES'; char_upper='YES'; 
 num_tol=.; date_tol_days=.; 
 time_tol_secs=.; datetime_tol_secs=.; delta_pct_tol=.; 
 exclude_from_report='NO'; comment='Trim+upper to suppress cosmetic diffs'; output; 

 /* Emails: case-insensitive compare, but do not trim */ 
 var_name='CONTACT_EMAIL'; 
 class_override='CHAR'; char_trim='NO'; char_upper='YES'; 
 num_tol=.; date_tol_days=.; time_tol_secs=.; datetime_tol_secs=.; delta_pct_tol=.; 
 exclude_from_report='NO'; comment='Upper only, preserve interior spaces if any'; output; 
/* Free-text notes—exclude from report noise */ 
 var_name='INTERNAL_NOTES';  class_override='CHAR'; char_trim='YES'; char_upper='NO'; 
 num_tol=.; date_tol_days=.; time_tol_secs=.; datetime_tol_secs=.; delta_pct_tol=.; 
 exclude_from_report='YES'; 
 comment='Operational notes excluded from Type/Value diffs'; output; 

 /* Vendor code: compare as-is (no normalization) */ 
 var_name='VENDOR_CODE';     class_override='CHAR'; char_trim='NO'; char_upper='NO'; 
 num_tol=.; 
 date_tol_days=.; time_tol_secs=.; datetime_tol_secs=.; delta_pct_tol=.; 
 exclude_from_report='NO'; comment='Exact-match string comparison'; output; 
run; 
/*Why this works:
 - Column names and meanings match what the macro reads (uppercases VAR_NAME internally).
 - For AMOUNT, setting both NUM_TOL and DELTA_PCT_TOL means a diff is flagged only when BOTH are exceeded.
 - DATE/TIME/DATETIME examples use their specific tolerance columns.
 - TRIM/UPPER are applied per variable before comparison when set to YES.
 - EXCLUDE_FROM_REPORT removes the variable from Type/Value diffs when respect_exclusions_tests12=YES (default).*/ 

/*---------------------------------------------------------------------------------------------------------
 VAR_CONFIG -- Example B (CSV for Excel users)
-----------------------------------------------------------------------------------------------------------

 Save as UTF-8 CSV and import (e.g., PROC IMPORT) into work.var_config before running the macro.
 VAR_NAME,CLASS_OVERRIDE,NUM_TOL,DATE_TOL_DAYS,TIME_TOL_SECS,DATETIME_TOL_SECS,DELTA_PCT_TOL,CHAR_TRIM,CHAR_UPPER,
 EXCLUDE_FROM_REPORT,COMMENT
 AMOUNT,NUMERIC,0.01,,,0.005,,,NO,"Money fields permit ±$0.01 and 0.5% relative-both must be exceeded"
 RATE,NUMERIC,0.000001,,,,,,,NO,"Tiny absolute differences only"
 NUM_TRANSACTIONS,NUMERIC,1,,,,,,,NO,"Off-by-1 tolerated for counters"
 CLOSE_DATE,DATE,,,1,,,,NO,"Date window of 1 day"
 CUTOVER_TIME,TIME,,,,2,,,NO,"Time window of 2 seconds"
 UPDATE_TS,DATETIME,,,,,2,,NO,"Datetime window of 2 seconds"
 CUSTOMER_NAME,CHAR,,,,,,,YES,YES,NO,"Trim+upper to suppress cosmetic diffs"
 CONTACT_EMAIL,CHAR,,,,,,,NO,YES,NO,"Upper only; preserve interior spaces if any"
 INTERNAL_NOTES,CHAR,,,,,,,YES,NO,YES,"Operational notes excluded from diffs"
 VENDOR_CODE,CHAR,,,,,,,NO,NO,NO,"Exact-match string comparison"*/ 

/*=========================================================================================================
 CONTROLS REFERENCE -- Supported in v1.0
===========================================================================================================

Required
  dA=, dB=, keys=
Outputs & Behavior
  outlib=work 
  var_config= 
  check_keys_unique=YES|NO              -> emits RECON_DUPLICATE_KEYS_A/B; 
  does NOT abort runs 
  respect_exclusions_tests12=YES|NO     -> drops excluded vars from Type/Value diffs 
  add_numeric_deltas=YES|NO             -> internal only; 
  deltas not written to output 
  add_wide_view=YES|NO                  -> builds WIDE only if VALUE_DIFF exists 
Keys-only presence
  keys_only_method=MERGE                -> only MERGE implemented; 
  other values fall back with a warning 
CSV Export (simple guard)
  export_csv=YES|NO 
  csv_dir= 
  csv_auto_guard=YES|NO 
  csv_max_rows=<integer> 
  csv_sample_on_guard=YES|NO            -> first N rows; 
  no seeded/stratified sampling
  csv_sample_include_typefmt=YES|NO
Notes
  longvals_enable= and value_len_max= are accepted but no dedicated long-value sidecars are created. 
  Final report value columns are fixed at $200 length.

===========================================================================================================
 TROUBLESHOOTING (symptom -> action)
===========================================================================================================

 - Dataset not found / key missing -> verify librefs, member names, keys exist in both A and B. 
 - Many TYPE/FORMAT diffs -> validate intended format changes; 
   normalize formats at source if desired.
 - Many cosmetic CHAR diffs -> add TRIM/UPPER in VAR_CONFIG for the specific variables. 
 - Excessive NUMERIC noise -> set NUM_TOL and/or DELTA_PCT_TOL per variable; 
   remember both must be exceeded if both set. 
 - WIDE not created -> ensure add_wide_view=YES AND at least one VALUE_DIFF exists. 
 - Summaries missing -> they exist when add_wide_view=YES; otherwise they are created during CSV export. 
 - CSV guard triggered -> only first N rows exported; if you need full data, increase csv_max_rows judiciously. 

===========================================================================================================
 QUALITY CONTROL CHECKLIST
===========================================================================================================

 - RECON_FINAL_REPORT counts agree with RECON_FINAL_SUMMARY_COUNTS (when present).
 - BY-KEY summary excludes rows with any missing key;
   confirm denominator matches distinct complete-key rows.
 - TYPE/FORMAT diffs are acknowledged and/or documented.
 - For CSV handoffs, document whether guard sampling occurred and provide parameters used.

===========================================================================================================
 QUICK COMMANDS
===========================================================================================================*/

 /* Counts (when created) */
 proc print data=work.recon_final_summary_counts; run; 
 /* Worst records */
 proc sort data=work.recon_final_summary_by_key; 
 by descending n_value_diffs; run; 
 proc print data=work.recon_final_summary_by_key(obs=50); run; 
 /* Noisiest variables (if CSV export was used and table exists) */
 proc sort data=work.recon_final_summary_by_variable; 
 by descending diff_rate descending n_keys_differ; run; 
 proc print data=work.recon_final_summary_by_variable(obs=50); run; 
 /* Run parameters audit */
 proc print data=work.recon_run_metadata; run; 

/*=========================================================================================================
 END OF JOB AID (v1.0-aligned)
===========================================================================================================*/
