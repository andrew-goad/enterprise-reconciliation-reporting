/*=====================================================================
  Test 3 of DATASET_RECONCILIATION_REPORT: KEYS-only presence (A xor B).

  Source: src/enterprise_reconciliation_diagnostic.sas (the keys-only MERGE
  block) run against the SIMPLE UAT datasets from
  tests/reconciliation_uat_validation.sas. The macro's &keys parameter is
  loan_id customer_id, so &_keys_clean resolves to "loan_id customer_id"
  and &outlib. to "work." -- substituted inline here so the block runs
  standalone, with the comparison logic exactly as written in the macro.

  Expected (per the UAT sign-off): KEY_ONLY_IN_ONE = 2 -- 2006/806 (Zed)
  present only in A, 2007/807 (Ivy) present only in B.
=====================================================================*/

/* ---- SIMPLE UAT input data (tests/reconciliation_uat_validation.sas) ---- */
data work.A;
  length loan_id 8 customer_id 8 name $40 amount 8 rate 8 post_ts 8; format post_ts datetime19.;
  input loan_id customer_id name $ amount rate post_ts :datetime19.; datalines;
2001 801 Alice     1000.00 0.035 01JAN2024:08:00:00
2002 802 Bob        999.99 0.035 01JAN2024:08:00:01
2003 803 Charlie   2500.00 0.040 02JAN2024:09:30:00
2006 806 Zed        500.00 0.033 04JAN2024:12:00:00
;
run;

data work.B; length loan_id 8 customer_id 8 name $40 amount 8 rate 8 post_ts 8; format post_ts datetime19.;
input loan_id customer_id name $ amount rate post_ts :datetime19.; datalines;
2001 801 ALICE     1000.00 0.035 01JAN2024:08:00:00
2002 802 BoB       1000.00 0.035 01JAN2024:08:00:01
2003 803 Charlie   2510.00 0.040 02JAN2024:09:30:00
2007 807 Ivy       2300.00 0.039 04JAN2024:11:00:00
;
run;

/* ---- KEYS-only presence via MERGE (src/enterprise_reconciliation_diagnostic.sas) ---- */
/* distinct keys from A and B to avoid false duplicates on merge */
proc sort data=work.A out=work._A_keys(keep=loan_id customer_id) nodupkey;
by loan_id customer_id; run;
proc sort data=work.B out=work._B_keys(keep=loan_id customer_id) nodupkey; by loan_id customer_id; run;

data work.RECON_KEYS_ONLY_IN_ONE;
    length category $24 item $32 valueA valueB $200 type $10 notes $200;
    merge work._A_keys(in=ina) work._B_keys(in=inb);
    by loan_id customer_id;
    if (ina ne inb);                       /* xor: present on one side only */
    category='KEY_ONLY_IN_ONE';
    item=''; valueA=''; valueB=''; type='';
    if ina and not inb then notes='Key only in A';
    else if inb and not ina then notes='Key only in B';
    else notes='';
    output;
run;

proc print data=work.RECON_KEYS_ONLY_IN_ONE noobs;
    var loan_id customer_id category notes;
    title "Keys present in only one dataset (Test 3)";
run;
