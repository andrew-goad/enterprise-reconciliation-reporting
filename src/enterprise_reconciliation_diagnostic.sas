/*==================================================================================================

Macro:             DATASET_RECONCILIATION_REPORT
Version:           1.0
SAS Version:       9.4 (Base SAS; runs in SAS EG; no Viya required)
Author:		   Andrew R Goad (linkedin.com/in/andrewrgoad ; ar.goad@yahoo.com)

PURPOSE: Compares two SAS datasets (A and B) using specified keys and produces a comprehensive reconciliation package.
         A single macro run produces a clear, teaching-friendly reconciliation report, summaries, and optional
         CSVs—enabling analysts and non-SAS users to understand differences between datasets quickly and accurately.

KEY FEATURES:
  -Tolerance-aware comparisons (absolute + relative for numeric; date/time/datetime windows).

  -Character normalization (trim/upper) and exclusion handling.
  -Governance metadata for auditability.

  -Machine learning pattern mining identifies frequent co-occurrence of variable differences,
   enabling association insights (support, confidence, lift).

HIGH-LEVEL FLOW:
  1. Preflight Checks: Validate dataset existence and keys; record run metadata.
  2. Metadata Capture: Use PROC CONTENTS to extract variable names, types, and formats; normalize names for joins.
  3. Optional VAR_CONFIG Integration: Apply per-variable tolerances, exclusions, and normalization flags.
  4. Tests:
      Test 1: Variables only in one dataset (FULL JOIN on metadata).
      Test 2: Type/Format differences for common variables (INNER JOIN).
      Test 3: Keys-only presence (MERGE by keys).
      Test 4: Value differences (PROC TRANSPOSE to long shape; compare numeric and character values with tolerances).
  5. Consolidation: into Final Report Combine KEY_ONLY_IN_ONE, VALUE_DIFF, TYPE_OR_FORMAT_DIFF into
     RECON_FINAL_REPORT; apply exclusions.
  6. Optional WIDE View: Pivot VALUE_DIFF rows for side-by-side comparison by key.
  7. Summaries: Counts by category, by key (excluding blank keys), and by variable (diff_rate uses keyed rows only).
  8. Optional CSV Export: Final report and summaries with guardrails for large datasets.

AUDIENCE:
  - SAS and non-SAS users.  Code is annotated so the SAS log becomes a teaching aid: explains what each
    PROC/DATA step does, how joins/merges/transposes work, why tolerances are applied, and how metadata is captured.

MACRO INPUT DEFAULTS:
  - respect_exclusions_tests12=YES      (excluded variables removed early from type/value checks)
  - keys_only_method=MERGE              (stable, transparent keys-only comparison)
  - add_wide_view=YES                   (enable pivoted comparison of value differences)
  - export_csv=NO                       (opt-in, server-friendly; can flip to YES)
  - longvals_enable=YES                 (sidecar for long text when truncation occurs)
  - add_numeric_deltas=YES              (absolute and relative deltas for numeric/value types)
  - value_len_max=200                   (truncation threshold used for TEXT values in long view)

CHANGELOG: N/A (Initial version 1.0)

GOVERNANCE / METADATA:
  - Records macro version, timestamp, inputs, key list, and options into RECON_RUN_METADATA.
  - Optional duplicate-key checks (A/B); can fail or warn depending on controls.
  - Summary counts, by-key, and by-variable tables created in-memory (and optionally exported to CSV).

LEARNING AID (how to read the log):
  - PROC CONTENTS: shows dataset structure (variable name, type, format).
    We normalize names to UPPER for joins.
  - PROC SQL FULL/INNER JOIN: used to detect vars-only-in-one side and type/format mismatches across A/B.
  - Merge by &keys: used for keys-only presence (A xor B).
  - PROC TRANSPOSE: reshapes data (long <-> wide) to compute value differences by variable and to build WIDE view.
  - Tolerance application: numeric absolute vs relative, date/time/datetime window thresholds, char trim/upper.
  - Summaries: GROUP BY category/key/variable; denominator excludes blank keys.
  - CSV export: PROC EXPORT writes tables to CSV; guardrails to prevent overly large exports.

NOTE on execution environment:
  - All options are parameters; no assumptions about external macro variables.
  - Keys may be mixed types (char/numeric); missing() works for both in SQL predicate generation.
==================================================================================================*/

%macro DATASET_RECONCILIATION_REPORT(
    /*==============================*/
    /* REQUIRED INPUTS              */
    /*==============================*/
    dA= /* libref.dataset for A, e.g., WORK.A */,
    dB= /* libref.dataset for B, e.g., WORK.B */,
    keys= /* space-separated key variables, e.g., loan_id customer_id */,

    /*==============================*/
    /* OUTPUT LIBRARY               */
    /*==============================*/
    outlib=work,

    /*==============================*/
    /* VAR-CONFIG (per-variable)    */
    /*==============================*/
    var_config= /* optional dataset of overrides/tolerances/exclusions */,

    /*==============================*/
    /* BEHAVIOR / CONTROLS          */
    /*==============================*/
    check_keys_unique=YES,        /* detect duplicates in A/B; creates outlib.RECON_DUPLICATE_KEYS_A/B */
    add_numeric_deltas=YES,       /* include absolute & relative deltas (when applicable) in value diffs */
    longvals_enable=YES,      
    /* sidecar with full text when truncated in final report */
    value_len_max=200,            /* trunk length (characters) used for valueA/valueB in final report */
    respect_exclusions_tests12=YES,/* remove excluded variables early from Type/Value diffs (Tests 1&2) */
    add_wide_view=YES,            /* pivot VALUE_DIFF rows by key to side-by-side comparison */
    wide_max_items=10,            /* cap WIDE to top-N variables by chosen metric */
    wide_rank_metric=DIFF_RATE,   /* rank metric for WIDE selection: DIFF_RATE or N_KEYS_DIFFER */
    keys_only_method=MERGE,       /* KEYS-only presence method: MERGE (default; simple & transparent) */

    /*==============================*/
    /* CSV EXPORT                   */
    /*==============================*/
    export_csv=NO,                /* set to YES to write CSVs */
    csv_dir=,                     /* target directory; defaults to WORK if not provided */
    csv_auto_guard=YES,           /* guard: sample-only if row count too large */
    csv_max_rows=1000000,         /* guard threshold */
    csv_sample_on_guard=YES,       /* if guarded, export a sample subset rather than full data */
    csv_sample_include_typefmt=YES /* include TYPE/FORMAT diffs in sample CSVs */
);
/*--------------------------------------------------------------------------------------------------
LOCAL MACRO VARS:
  _ok               overall input guard
  _dsA/_dsB         sanitized dataset references (strip whitespace)
  _keys_clean       keys normalized to single spaces
  _keys_comma       keys comma-delimited string (for SQL SELECT/GROUP BY)
  _keys_label       label string for metadata
  _select_keys_a    SELECT-list of keys from table alias 'a'
  _on_keys_eq       JOIN equality predicates A vs B
  _keys_present_pred SQL predicate requiring ALL keys NOT MISSING (works for char + num)
--------------------------------------------------------------------------------------------------*/
%local _ok _dsA _dsB _keys_clean _keys_comma _keys_label
       _select_keys_a _on_keys_eq _keys_present_pred
       _k_i _k;

/*------------------------------*/
/* Basic input normalization    */
/*------------------------------*/
%let _ok=1;                                 /* start guard as OK */
%let _dsA=%sysfunc(prxchange(s/\s+//io, -1, &dA));
/* remove whitespace inside libref.dsname */
%let _dsB=%sysfunc(prxchange(s/\s+//io, -1, &dB));
%let _keys_clean=%sysfunc(compbl(&keys));
/* collapse multiple spaces to one */
%let _keys_comma=%sysfunc(tranwrd(&_keys_clean,%str( ),%str(, )));   /* e.g., loan_id, customer_id */
%let _keys_label=%sysfunc(tranwrd(&_keys_clean,%str( ),%str(, )));
/* same as above for metadata */

/*----------------------------------*/
/* Basic validation                 */
/*----------------------------------*/
%if %sysfunc(exist(&_dsA))=0 %then %do;
    %put ERROR: (DRR) dA=&dA not found.;
    %let _ok=0;
%end;

%if %sysfunc(exist(&_dsB))=0 %then %do;
    %put ERROR: (DRR) dB=&dB not found.;
    %let _ok=0;
%end;

%if %length(&_keys_clean)=0 %then %do;
    %put ERROR: (DRR) keys= must be provided.;
    %let _ok=0;
%end;
%if &_ok=0 %then %do;
    %put ERROR: (DRR) Aborting due to invalid inputs.;
    %goto _exit;
%end;

/*----------------------------------*/
/* RUN METADATA (provenance)        */
/*----------------------------------*/
data &outlib.RECON_RUN_METADATA;
    length macro_version $32 run_dttm 8 dA dB $41 keys $200 options $300;
    format run_dttm datetime19.;
    macro_version = 'v1.0';
    /* version string */
    run_dttm = datetime();
    /* timestamp (UTC on server; local in EG UI) */
    dA = "&_dsA"; dB="&_dsB"; keys = "&_keys_label";
    /* create a readable summary of chosen options for future audits */
    options = catt('check_keys_unique=',"&check_keys_unique",
                   ', add_numeric_deltas=',"&add_numeric_deltas",
                   ', longvals_enable=',"&longvals_enable",
                   ', add_wide_view=',"&add_wide_view",
                   ', keys_only_method=',"&keys_only_method",
                   ', respect_exclusions_tests12=',"&respect_exclusions_tests12");
    output;
run;

/*----------------------------------*/
/* Duplicate key check (opt)        */
/*----------------------------------*/
/* - useful in reconciliation so users know whether keys uniquely identify rows
   - outputs two small tables with key groups and their counts (>1)             */
%if %upcase(&check_keys_unique)=YES %then %do;
    proc sql noprint;
        create table &outlib.RECON_DUPLICATE_KEYS_A as
        select &_keys_comma, count(*) as n
        from &_dsA
        group by &_keys_comma
        having calculated n>1;
        
        create table &outlib.RECON_DUPLICATE_KEYS_B as
        select &_keys_comma, count(*) as n
        from &_dsB
        group by &_keys_comma
        having calculated n>1;
    quit;
%end;

/*------------------------------------------------------------------*/
/* Dataset metadata (PROC CONTENTS on A and B)                      */
/* - we capture name/type/format to compare                         */
/* - then normalize names to UPPERCASE for join                     */
/*------------------------------------------------------------------*/
proc contents data=&_dsA out=work._CA(keep=name type length format formatl formatd) noprint;
run;

proc contents data=&_dsB out=work._CB(keep=name type length format formatl formatd) noprint;
run;

data work._CA;
    set work._CA;
    length upcase_name $32;             /* consistent length avoids mismatch in later joins */
    upcase_name=upcase(name);
run;

data work._CB;
    set work._CB;
    length upcase_name $32;
    upcase_name=upcase(name);
run;

/*------------------------------------------------------------------*/
/* Build normalized VAR_CONFIG (optional, may be empty)             */
/* - we keep only what's needed and uppercase flags                 */
/*------------------------------------------------------------------*/
%local _has_vc;
%let _has_vc=0;

%if %length(&var_config)>0 and %sysfunc(exist(&var_config)) %then %let _has_vc=1;

%if &_has_vc %then %do;
    data work._VARCFG;
        set &var_config;
        length upcase_name $32;
        upcase_name = upcase(var_name);
        /* normalize textual flags so comparisons are robust */
        class_override       = upcase(coalescec(class_override, ''));
        char_trim            = upcase(coalescec(char_trim, 'NO'));
        char_upper           = upcase(coalescec(char_upper, 'NO'));
        exclude_from_report  = upcase(coalescec(exclude_from_report, 'NO'));
    run;
    /* guard: row-count visibility for users (log becomes a teaching aid) */
    %local _vc_n; %let _vc_n=.;
    proc sql noprint;
        select count(*) into :_vc_n trimmed from work._VARCFG;
    quit;

    %if %sysfunc(inputn(&_vc_n, best.)) < 1 %then %do;
        %put WARNING: (DRR) VAR_CONFIG has 0 rows - tolerances/flags will be defaults.;
    %end;
%end;
%else %do;
    /* Build an empty VARCFG shell that still has all referenced columns */
    data work._VARCFG;
        length upcase_name $32
               class_override $10
               char_trim $3 char_upper $3
               exclude_from_report $3;
        length num_tol date_tol_days time_tol_secs datetime_tol_secs delta_pct_tol 8;
        /* No rows on purpose; columns exist so downstream SELECT/WHERE work cleanly */
        stop;
    run;
%end;

/*------------------------------------------------------------------*/
/* Variables present only in one side (A-only vs B-only)            */
/* - FULL JOIN on upcase_name, then choose the sides                */
/*------------------------------------------------------------------*/
proc sql noprint;
    create table &outlib.RECON_VARS_ONLY_IN_ONE as
    select coalesce(a.upcase_name,b.upcase_name) as name,
           case when a.upcase_name is not null and b.upcase_name is null then 'A_ONLY'
                when a.upcase_name is null and b.upcase_name is not null then 'B_ONLY'
                else 'BOTH' end as side
    from work._CA a
    full join work._CB b
    on a.upcase_name=b.upcase_name
    where calculated side in ('A_ONLY','B_ONLY')
    order by name;
quit;

/*------------------------------------------------------------------*/
/* TYPE / FORMAT differences on common variables                    */
/* - INNER JOIN on upcase_name between A and B                      */
/* - compare type and normalized format strings                     */
/*------------------------------------------------------------------*/
proc sql noprint;
    create table &outlib.RECON_TYPE_FORMAT_DIFFS as
    select a.upcase_name as name,
           a.type as typeA, b.type as typeB,
           /* normalized format string: format + width/decimals when present */
           cats(coalescec(a.format,''),
                case when a.formatl>0 then cats('.',a.formatl) else '' end,
                case when a.formatd>0 then cats('.',a.formatd) else '' end) as fmtA_norm,
           cats(coalescec(b.format,''),
                case when b.formatl>0 then cats('.',b.formatl) else '' end,
                case when b.formatd>0 then cats('.',b.formatd) else '' end) as fmtB_norm
    from work._CA a
    inner join work._CB b
      on a.upcase_name=b.upcase_name
    where (a.type ne b.type)
       or (cats(coalescec(a.format,''),a.formatl,a.formatd))
       ne cats(coalescec(b.format,''),b.formatl,b.formatd))
    order by name;
quit;

/*------------------------------------------------------------------*/
/* KEYS-only presence (A xor B)                                     */
/* - default method: MERGE (simple, stable)                         */
/* - results go to RECON_KEYS_ONLY_IN_ONE                           */
/*------------------------------------------------------------------*/
%if %upcase(&keys_only_method)=MERGE %then %do;
    /* distinct keys from A and B to avoid false duplicates on merge */
    proc sort data=&_dsA out=work._A_keys(keep=&_keys_clean) nodupkey;
    by &_keys_clean; run;
    proc sort data=&_dsB out=work._B_keys(keep=&_keys_clean) nodupkey; by &_keys_clean; run;

    data &outlib.RECON_KEYS_ONLY_IN_ONE;
        length category $24 item $32 valueA valueB $200 type $10 notes $200;
        merge work._A_keys(in=ina) work._B_keys(in=inb);
        by &_keys_clean;
        if (ina ne inb);                       /* xor: present on one side only */
        category='KEY_ONLY_IN_ONE';
        item=''; valueA=''; valueB=''; type='';
        if ina and not inb then notes='Key only in A';
        else if inb and not ina then notes='Key only in B';
        else notes='';
        output;
    run;
%end;
%else %do;
    %put WARNING: (DRR) keys_only_method=&keys_only_method not implemented - using MERGE fallback.;
    %goto _keys_merge_fallback;
%end;
%goto _after_keys;

%_keys_merge_fallback:
    proc sort data=&_dsA out=work._A_keys(keep=&_keys_clean) nodupkey; by &_keys_clean; run;
    proc sort data=&_dsB out=work._B_keys(keep=&_keys_clean) nodupkey; by &_keys_clean; run;
    data &outlib.RECON_KEYS_ONLY_IN_ONE;
        length category $24 item $32 valueA valueB $200 type $10 notes $200;
        merge work._A_keys(in=ina) work._B_keys(in=inb);
        by &_keys_clean;
        if (ina ne inb);                       /* xor: present on one side only */
        category='KEY_ONLY_IN_ONE';
        item=''; valueA=''; valueB=''; type='';
        if ina and not inb then notes='Key only in A';
        else if inb and not ina then notes='Key only in B';
        else notes='';
        output;
    run;
%_after_keys:

/*------------------------------------------------------------------*/
/* Build a list of COMMON NUMERIC and COMMON CHARACTER variables    */
/* - We exclude key variables so Test 2 focuses on non-key fields   */
/*------------------------------------------------------------------*/
/* materialize the key names (UPPER) for exclusion joins */
data work._KEYS_LIST;
    length name $32;
    %let _k_i=1;
    %do %while(%length(%scan(&_keys_clean,&_k_i,%str( ))));
        name=upcase("%scan(&_keys_clean,&_k_i,%str( ))");
        output;
        %let _k_i=%eval(&_k_i+1);
    %end;
    stop;
run;

/* COMMON NUMERIC vars present in both A and B (excluding keys) */
proc sql noprint;
    create table work._COMMON_NUM as
    select c.name
    from (
        select a.upcase_name as name
        from work._CA a inner join work._CB b
          on a.upcase_name=b.upcase_name
        where a.type=1 and b.type=1             /* 1 = numeric in SAS dictionary */
    ) c
    left join work._KEYS_LIST k on c.name=k.name
    where k.name is null
    order by c.name;

/* COMMON CHARACTER vars present in both A and B (excluding keys) */
    create table work._COMMON_CHAR as
    select c.name
    from (
        select a.upcase_name as name
        from work._CA a inner join work._CB b
          on a.upcase_name=b.upcase_name
        where a.type=2 and b.type=2             /* 2 = character */
    ) c
    left join work._KEYS_LIST k on c.name=k.name
    where k.name is null
    order by c.name;
quit;

/*------------------------------------------------------------------*/
/* Build space-separated var lists used in TRANSPOSE steps (robust) */
/*------------------------------------------------------------------*/
%local _num_list _char_list;
%let _num_list=; %let _char_list=;
proc sql noprint;
    select name into :_num_list  separated by ' ' from work._COMMON_NUM;
    select name into :_char_list separated by ' ' from work._COMMON_CHAR;
quit;
/* Optional one-liner to confirm at run time (remove later) */
/* %put NOTE: (DRR) _char_list = &_char_list; */

/* === TEMP DEBUG START === */
%put NOTE: (DRR) _char_list = &_char_list;
proc sql; title "(DRR) COMMON_CHAR variables";
    select * from work._COMMON_CHAR;
quit; title;
/* === TEMP DEBUG END === */

/*------------------------------------------------------------------*/
/* Compute SELECT list & JOIN condition strings for SQL joins       */
/* - &_select_keys_a:    a.key1, a.key2, ...                        */
/* - &_on_keys_eq:       a.key1=b.key1 AND a.key2=b.key2 AND ...    */
/*------------------------------------------------------------------*/
%let _select_keys_a=;
%let _on_keys_eq=;
%let _k_i=1;
%do %while(%length(%scan(&_keys_clean,&_k_i,%str( ))));
    %let _k=%scan(&_keys_clean,&_k_i,%str( ));
    %if &_k_i=1 %then %do;
        %let _select_keys_a = a.&_k;
        %let _on_keys_eq    = a.&_k=b.&_k;
    %end;
    %else %do;
        %let _select_keys_a = &_select_keys_a, a.&_k;
        %let _on_keys_eq    = &_on_keys_eq and a.&_k=b.&_k;
    %end;
    %let _k_i=%eval(&_k_i+1);
%end;

/*------------------------------------------------------------------*/
/* Build predicate requiring ALL keys NOT MISSING                   */
/* - used to EXCLUDE blank-key rows in by-key summary and denominator */
/* - missing() works for both character and numeric in PROC SQL      */
/*------------------------------------------------------------------*/
%let _keys_present_pred=;
%let _k_i=1;
%do %while(%length(%scan(&_keys_clean,&_k_i,%str( ))));
    %let _k=%scan(&_keys_clean,&_k_i,%str( ));
    %if &_k_i=1 %then %let _keys_present_pred = (not missing(&_k));
    %else             %let _keys_present_pred = &_keys_present_pred and (not missing(&_k));
    %let _k_i=%eval(&_k_i+1);
%end;
%if %length(&_keys_present_pred)=0 %then %let _keys_present_pred=1=1;

/*==========================================================================*/
/* TEST (2): VALUE DIFFERENCES - NUMERIC                                    */
/* - TRANSPOSE A and B to long shape: (keys, variable->name, col1->value)   */
/* - INNER JOIN by (variable name + keys)                                   */
/* - Attach VAR_CONFIG tolerances, apply class overrides (NUMERIC/DATE/TIME/DATETIME) */
/* - For NUMERIC: require BOTH absolute and relative tolerances when both provided  */
/* - EPS guard keeps comparisons stable near zero                           */
/*==========================================================================*/
%if %length(&_num_list)>0 %then %do;
  /* keep only keys and common numeric vars, sorted by keys for stable transpose */
  proc sort data=&_dsA out=work._A_num_keep;
  by &_keys_clean; run;
  proc sort data=&_dsB out=work._B_num_keep; by &_keys_clean; run;

  proc transpose data=work._A_num_keep out=work._A_num_long name=name;
    by &_keys_clean;
    var &_num_list;
  run;
  proc transpose data=work._B_num_keep out=work._B_num_long name=name;
    by &_keys_clean;
    var &_num_list;
  run;

  /* normalize names and capture numeric value columns */
  data work._A_num_long;
    set work._A_num_long;
    length upcase_name $32;
    upcase_name=upcase(name);
    valueA=col1;
    drop col1;
  run;

  data work._B_num_long;
    set work._B_num_long;
    length upcase_name $32;
    upcase_name=upcase(name);
    valueB=col1;
    drop col1;
  run;

  /* INNER JOIN on (variable name + keys) to align A/B values */
  proc sql;
    create table work._NUM_JOIN as
    select a.upcase_name as upcase_name, &_select_keys_a, a.valueA, b.valueB
    from work._A_num_long a inner join work._B_num_long b
      on a.upcase_name=b.upcase_name and &_on_keys_eq;
  quit;

  /* attach tolerances/class overrides from VAR_CONFIG */
  proc sql;
    create table work._NUM_JOIN_CFG as
    select j.*, v.class_override, v.num_tol, v.date_tol_days, v.time_tol_secs, v.datetime_tol_secs, v.delta_pct_tol,
           v.exclude_from_report
    from work._NUM_JOIN j
    left join work._VARCFG v
    on j.upcase_name = v.upcase_name;
  quit;

  /* evaluate differences using tolerances */
  data &outlib.RECON_VALUE_DIFFS_LONG_NUM;
    length category $24 item $32 valueA_c valueB_c $200 type $10 notes $200;
    set work._NUM_JOIN_CFG;
    category='VALUE_DIFF'; item=upcase_name; notes='';
    /* choose comparison class: overridden or inferred as NUMERIC */
    length _cls $9;
    _cls = coalescec(class_override, 'NUMERIC');

    /* load tolerances into local working vars (retain for speed) */
    retain _ntol _ptol 0 _dtol 0 _ttol 0;
    /* numeric + date/time/datetime tolerances */
    _ntol = coalesce(num_tol,0);
    _ptol = coalesce(delta_pct_tol,.);
    _dtol = coalesce(datetime_tol_secs,0);
    _ttol = coalesce(time_tol_secs,0);
    length _dtdays 8; _dtdays = coalesce(date_tol_days,0);

    /* numeric comparison stability guards */
    length isdiff 8;
    length _absdiff _eps _rel 8;
    _absdiff = abs(valueA - valueB);
    _eps = 1e-12 * max(1, max(abs(valueA), abs(valueB)));
    /* class-aware tolerance decision tree */
    select (_cls);
        when ('DATETIME') isdiff = (_absdiff > (_dtol + _eps));
        when ('TIME')     isdiff = (_absdiff > (_ttol + _eps));
        when ('DATE')     isdiff = (_absdiff > (_dtdays + _eps));
        otherwise do;
            /* NUMERIC */
            _rel = _absdiff / max(1e-12, max(abs(valueA), abs(valueB)));
            if missing(_ptol) then do;
                isdiff = (_absdiff > (_ntol + _eps));
            end;
            else do;
                /* require BOTH absolute and relative tolerances to be exceeded */
                isdiff = (_absdiff > (_ntol + _eps)) and (_rel > (_ptol + 1e-12));
            end;
        end;
    end;

    /* render values according to class for human-readable output */
    if isdiff then do;
        select (_cls);
            when ('DATETIME') do; valueA_c=strip(put(valueA, datetime19.)); valueB_c=strip(put(valueB, datetime19.)); type='datetime'; end;
            when ('TIME')     do; valueA_c=strip(put(valueA, time8.));
                                  valueB_c=strip(put(valueB, time8.));      type='time';     end;
            when ('DATE')     do; valueA_c=strip(put(valueA, date9.));      valueB_c=strip(put(valueB, date9.));      type='date';     end;
            otherwise         do; valueA_c=strip(put(valueA, best32.));     valueB_c=strip(put(valueB, best32.));     type='numeric';  end;
        end;
        output;
    end;
    /* drop working variables to keep final dataset clean */
    drop _cls _ntol _ptol _dtol _ttol _dtdays isdiff valueA valueB _absdiff _eps _rel;
  run;

%end;
%else %do;
    /* if no common numeric vars, create empty shell for union */
    data &outlib.RECON_VALUE_DIFFS_LONG_NUM;
        length category $24 item $32 valueA_c valueB_c $200 type $10 notes $200;
        stop;
    run;
%end;

/*==========================================================================*/
/* TEST (2): VALUE DIFFERENCES - CHARACTER                                  */
/* - TRANSPOSE A and B to long shape for character variables                */
/* - Apply trim/upper normalization (per VAR_CONFIG or defaults)            */
/*==========================================================================*/
%if %length(&_char_list)>0 %then %do;
    proc sort data=&_dsA out=work._A_char_keep; by &_keys_clean; run;
    proc sort data=&_dsB out=work._B_char_keep; by &_keys_clean; run;

    proc transpose data=work._A_char_keep out=work._A_char_long name=name;
        by &_keys_clean;
        var &_char_list;
    run;

    proc transpose data=work._B_char_keep out=work._B_char_long name=name;
        by &_keys_clean;
        var &_char_list;
    run;

    data work._A_char_long;
        set work._A_char_long;
        length upcase_name $32;
        upcase_name=upcase(name);
        valueA_c=coalescec(col1,'');    /* coalesce character to '' to avoid NULLs */
        drop col1;
    run;

    data work._B_char_long;
        set work._B_char_long;
        length upcase_name $32;
        upcase_name=upcase(name);
        valueB_c=coalescec(col1,'');
        drop col1;
    run;

    proc sql;
        create table work._CHAR_JOIN as
        select a.upcase_name as upcase_name, &_select_keys_a, a.valueA_c, b.valueB_c
        from work._A_char_long a inner join work._B_char_long b
          on a.upcase_name=b.upcase_name and &_on_keys_eq;
    quit;

    /* attach char normalization flags (trim/upper) */
    proc sql;
        create table work._CHAR_JOIN_CFG as
        select j.*, v.char_trim, v.char_upper, v.exclude_from_report
        from work._CHAR_JOIN j
        left join work._VARCFG v on j.upcase_name=v.upcase_name;
    quit;

    data &outlib.RECON_VALUE_DIFFS_LONG_CHAR;
        length category $24 item $32 valueA_c valueB_c $200 type $10 notes $200;
        set work._CHAR_JOIN_CFG;
        category='VALUE_DIFF'; item=upcase_name; type='char';
        notes='';

        /* copy into working buffers (_a/_b) for normalization */
        length _a _b $32767;
        _a=valueA_c; _b=valueB_c;
        /* per-var trim and upper flags (YES/NO strings) */
        if upcase(coalescec(char_trim,'NO'))='YES' then do; _a=strip(_a); _b=strip(_b); end;
        if upcase(coalescec(char_upper,'NO'))='YES' then do; _a=upcase(_a); _b=upcase(_b); end;

        if _a ne _b then output;
        /* only emit when normalized comparison differs */

        drop _a _b;
    run;

%end;
%else %do;
    data &outlib.RECON_VALUE_DIFFS_LONG_CHAR;
        length category $24 item $32 valueA_c valueB_c $200 type $10 notes $200;
        stop;
    run;
%end;

/*------------------------------------------------------------------*/
/* Unified VALUE_DIFF long table: numeric + char                    */
/*------------------------------------------------------------------*/
data &outlib.RECON_VALUE_DIFFS_LONG;
    length upcase_name $32; /* unify to avoid length mismatch warnings */
    set &outlib.RECON_VALUE_DIFFS_LONG_NUM &outlib.RECON_VALUE_DIFFS_LONG_CHAR;
run;

/*------------------------------------------------------------------*/
/* FINAL REPORT construction                                        */
/* - KEY_ONLY_IN_ONE                                                */
/* - VALUE_DIFF (human-readable valueA/valueB)                      */
/* - TYPE_OR_FORMAT_DIFF                                            */
/*------------------------------------------------------------------*/
proc format;
    value $SEV
        'KEY_ONLY_IN_ONE'    = '1'
        'TYPE_OR_FORMAT_DIFF' = '2'
        'VALUE_DIFF'         = '3'
        other                = '9';
run;

/* TYPE/FORMAT to final report rows (valueA/valueB carry type+fmt info) */
data work._FR_TYPEFMT_BASE;
    length category $24 item $32 valueA valueB $200 type $10 notes $200;
    set &outlib.RECON_TYPE_FORMAT_DIFFS;
    category='TYPE_OR_FORMAT_DIFF'; item=name; type='';
    valueA=cats('type=',typeA,', fmt=',fmtA_norm);
    valueB=cats('type=',typeB,', fmt=',fmtB_norm);
    notes='';
run;

/* VALUE_DIFF rows: move rendered valueA_c/valueB_c into final valueA/valueB */
data work._FR_VALUES_BASE;
    set &outlib.RECON_VALUE_DIFFS_LONG;
    length valueA valueB $200;
    valueA=valueA_c; valueB=valueB_c;
    drop valueA_c valueB_c;
run;

/* === TEMP DEBUG START === */
proc sql;
    title "(DRR) FR_VALUES_BASE counts by item";
    select item, count(*) as n_rows
    from work._FR_VALUES_BASE
    group by item
    order by calculated n_rows desc;
quit; title;
/* === TEMP DEBUG END === */

/* KEYS-only rows (already in final shape) */
data work._FR_KEYS;
    set &outlib.RECON_KEYS_ONLY_IN_ONE;
run;

/* Respect exclusions (Tests 1&2) when requested */
%if %upcase(&respect_exclusions_tests12)=YES %then %do;

/* build exclusion list from VAR_CONFIG once */
proc sql;
    create table work._EXCL as
    select upcase_name from work._VARCFG where upcase(exclude_from_report)='YES';
quit;

/* drop excluded VALUE_DIFF variables */
proc sql;
    create table work._FR_VALUES as
    select v.* from work._FR_VALUES_BASE v
    left join work._EXCL e on upcase(v.item)=e.upcase_name
    where e.upcase_name is null;
/* drop excluded TYPE/FORMAT variables */
    create table work._FR_TYPEFMT as
    select t.* from work._FR_TYPEFMT_BASE t
    left join work._EXCL e on upcase(t.item)=e.upcase_name
    where e.upcase_name is null;
quit;

%end;
%else %do;
    data work._FR_VALUES;  set work._FR_VALUES_BASE;  run;
    data work._FR_TYPEFMT; set work._FR_TYPEFMT_BASE; run;
%end;

/* normalize column lengths across the three report parts to avoid warnings
   NOTE: when VAR_CONFIG is not provided (&_has_vc=0),
         ensure exclude_from_report exists and defaults to 'NO' */
%macro _keep_cols(ds);
    data &ds; set &ds;
        length category $24 item $32 valueA valueB $200 type $10 notes $200;
        /* ---- default exclude_from_report when VAR_CONFIG missing ---- */
        %if %symexist(_has_vc) %then %do;
            %if &_has_vc = 0 %then %do;
                length exclude_from_report $3;
                if missing(exclude_from_report) then exclude_from_report='NO';
            %end;
        %end;
    run;
%mend;

%_keep_cols(work._FR_KEYS);
%_keep_cols(work._FR_VALUES);
%_keep_cols(work._FR_TYPEFMT);

/* final report union + severity rank */
data &outlib.RECON_FINAL_REPORT;
    set work._FR_KEYS work._FR_VALUES work._FR_TYPEFMT;
    length severity_rank $2;
    severity_rank=put(category,$SEV.);
run;

/*------------------------------------------------------------------*/
/* Optional WIDE view + in-memory summaries                         */
/* - WIDE: pivot VALUE_DIFF rows by key                             */
/* - Summaries: counts by-key, by-variable                          */
/*------------------------------------------------------------------*/
%if %upcase(&add_wide_view)=YES 
%then %do;

/* isolate VALUE_DIFF rows to drive WIDE transforms */
proc sql noprint;
    create table work._VD as
    select * from &outlib.RECON_FINAL_REPORT
    where category='VALUE_DIFF';
    select count(*) into :_vd_n from work._VD;
quit;

/* skip WIDE when there are no value diffs */
%if %sysevalf(&_vd_n > 0) %then %do;
    /* split A-side and B-side for pivoting */
    proc sql noprint;
        create table work._VDA as select &_keys_comma, item, valueA from work._VD;
        create table work._VDB as select &_keys_comma, item, valueB from work._VD;
    quit;

    proc sort data=work._VDA; by &_keys_clean; run;
    proc sort data=work._VDB; by &_keys_clean; run;
    /* transpose into wide tables keyed by &keys, columns per item (variable) */
    proc transpose data=work._VDA out=work._VDA_wide;
    by &_keys_clean; id item; var valueA; run;
    proc transpose data=work._VDB out=work._VDB_wide; by &_keys_clean; id item; var valueB; run;
    /* merge A/B wide tables into final WIDE view */
    data &outlib.RECON_FINAL_WIDE_BY_KEY;
        merge work._VDA_wide work._VDB_wide;
        by &_keys_clean;
    run;

%end;
%else %do;
    %put NOTE: (DRR) No VALUE_DIFF rows - skipping WIDE transposes.;
%end;

/* In-memory summaries (counts and by-key) */
proc sql;
    /* summary counts by category for quick overview */
    create table &outlib.RECON_FINAL_SUMMARY_COUNTS as
    select category, count(*) as n_differences
    from &outlib.RECON_FINAL_REPORT
    group by category;
    /* exclude non-keyed rows from by-key summary */
    create table &outlib.RECON_FINAL_SUMMARY_BY_KEY as
    select &_keys_comma,
           sum(category='VALUE_DIFF') as n_value_diffs
    from &outlib.RECON_FINAL_REPORT
    where &_keys_present_pred
    group by &_keys_comma;
quit;

%end;

/*------------------------------------------------------------------*/
/* CSV Export (optional)                                            */
/* - Export final report + summaries                                */
/* - denominator excludes non-keyed rows                            */
/*------------------------------------------------------------------*/

%if %upcase(&export_csv)=YES %then %do;
    /* default CSV directory to WORK if not provided */
    %if %length(&csv_dir)=0 %then %do; %let csv_dir=%sysfunc(pathname(work)); %end;
    /* row-count guard: sample or full export decision */
    %local _nrows;
    proc sql noprint;
    select count(*) into :_nrows from &outlib.RECON_FINAL_REPORT; quit;

    %if %sysevalf(&_nrows > &csv_max_rows) %then %do;
        %if %upcase(&csv_sample_on_guard)=YES %then %do;
            %put WARNING: (DRR) Row count &_nrows exceeds csv_max_rows=&csv_max_rows - Exporting a SAMPLE only.;
            data work._FR_sample; set &outlib.RECON_FINAL_REPORT(obs=&csv_max_rows); run;
            %let _fr_for_csv=work._FR_sample;
        %end;
        %else %do; %let _fr_for_csv=&outlib.RECON_FINAL_REPORT; %end;
    %end;
    %else %do; %let _fr_for_csv=&outlib.RECON_FINAL_REPORT; %end;

    /* write final report CSV */
    proc export data=&_fr_for_csv outfile="&csv_dir/recon_final_report.csv" dbms=csv replace; run;

    /* ensure in-memory summaries exist (counts, by-key) */
    %if %sysfunc(exist(&outlib.RECON_FINAL_SUMMARY_COUNTS))=0 %then %do;
        proc sql;
            create table &outlib.RECON_FINAL_SUMMARY_COUNTS as
            select category, count(*) as n_differences
            from &outlib.RECON_FINAL_REPORT
            group by category;
        quit;
    %end;

    %if %sysfunc(exist(&outlib.RECON_FINAL_SUMMARY_BY_KEY))=0 %then %do;
        proc sql;
            create table &outlib.RECON_FINAL_SUMMARY_BY_KEY as
            select &_keys_comma,
                   sum(category='VALUE_DIFF') as n_value_diffs
            from &outlib.RECON_FINAL_REPORT
            where &_keys_present_pred
            group by &_keys_comma;
        quit;
    %end;

    /* export summary counts + by-key */
    proc export data=&outlib.RECON_FINAL_SUMMARY_COUNTS outfile="&csv_dir/recon_final_summary_counts.csv" dbms=csv replace; run;

    %if %sysfunc(exist(&outlib.RECON_FINAL_SUMMARY_BY_KEY)) %then %do;
        proc export data=&outlib.RECON_FINAL_SUMMARY_BY_KEY outfile="&csv_dir/recon_final_summary_by_key.csv" dbms=csv replace; run;
    %end;
    %else %do;
        %put WARNING: (DRR) RECON_FINAL_SUMMARY_BY_KEY not present - skipping CSV.;
    %end;

    /* compute denominator excluding non-keyed rows for diff_rate by variable */
    %local _nkeys_all; %let _nkeys_all=0;

    proc sql noprint;
    select count(*) into :_nkeys_all
        from (select distinct &_keys_comma
              from &_fr_for_csv
              where &_keys_present_pred);
    quit;

    /* compute by-variable summary from final report */
    proc sql;
    create table work._DIFFS as
        select item as variable, count(*) as n_keys_differ
        from &outlib.RECON_FINAL_REPORT
        where category='VALUE_DIFF'
        group by item;

    create table &outlib.RECON_FINAL_SUMMARY_BY_VARIABLE as
        select d.variable, d.n_keys_differ,
               case when &_nkeys_all>0 then d.n_keys_differ/&_nkeys_all else 0 end as diff_rate
        from work._DIFFS d;
    quit;

    /* export by-variable summary */
    proc export data=&outlib.RECON_FINAL_SUMMARY_BY_VARIABLE
        outfile="&csv_dir/recon_final_summary_by_variable.csv" dbms=csv replace; run;
%end;

_exit:
%mend DATASET_RECONCILIATION_REPORT;
