/*=====================================================================
  Test 1 of DATASET_RECONCILIATION_REPORT: variables present only in
  one side (schema drift, A-only vs B-only).

  Source: src/enterprise_reconciliation_diagnostic.sas (the PROC CONTENTS
  metadata capture + name normalization + FULL JOIN vars-only-in-one
  block) run against the RICH UAT datasets from
  tests/reconciliation_uat_validation.sas, where A carries EXTRA_A and B
  carries EXTRA_B. &outlib. is resolved to "work." so the block runs
  standalone, with the metadata logic exactly as written in the macro.

  Expected: RECON_VARS_ONLY_IN_ONE = EXTRA_A (A_ONLY) and EXTRA_B (B_ONLY).
=====================================================================*/

/* ---- RICH UAT input data (tests/reconciliation_uat_validation.sas) ---- */
data work.A;
  length loan_id 8 customer_id 8 name $40 amount 8 rate 8 post_ts 8 description $500 code 8 extra_a $20;
  format post_ts datetime19.; format amount dollar10.2;
  loan_id=2001; customer_id=801; name='Alice '; amount=1000.00; rate=0.035; post_ts='01JAN2024:08:00:00'dt;
    description=repeat('Lorem ipsum ',25); code=123; extra_a='A_only'; output;
  loan_id=2002; customer_id=802; name='Bob'; amount=999.99; rate=0.0350000; post_ts='01JAN2024:08:00:01'dt;
    description=repeat('Alpha ',20); code=456; extra_a='A_only'; output;
  loan_id=2003; customer_id=803; name='Charlie'; amount=2500.00; rate=0.0400000; post_ts='02JAN2024:09:30:00'dt;
    description=repeat('Delta ',30); code=789; output;
  loan_id=2004; customer_id=804; name='Dana'; amount=3000.00; rate=0.0410000; post_ts='02JAN2024:09:30:00'dt;
    description=repeat('Foxtrot ',25); code=111; output;
  loan_id=2005; customer_id=805; name='Evan'; amount=1500.00; rate=0.0400005; post_ts='03JAN2024:07:15:00'dt;
    description=repeat('Golf ',40); code=222; output;
  loan_id=2010; customer_id=810; name='Dup'; amount=999.00; rate=0.038; post_ts='05JAN2024:10:00:00'dt;
    description=repeat('Duplicate ',10); code=999; output;
  loan_id=2010; customer_id=810; name='Dup'; amount=999.50; rate=0.038; post_ts='05JAN2024:10:00:00'dt;
    description=repeat('Duplicate ',10); code=999; output;
run;

data work.B;
  length loan_id 8 customer_id 8 name $40 amount 8 rate 8 post_ts 8 description $500 code $5 extra_b $20;
  format post_ts datetime19.; format amount dollar12.2;
  loan_id=2001; customer_id=801; name='ALICE'; amount=1000.00; rate=0.035; post_ts='01JAN2024:08:00:00'dt;
    description=repeat('Lorem ipsum ',25); code='123'; extra_b='B_only'; output;
  loan_id=2002; customer_id=802; name='BoB'; amount=1000.00; rate=0.0350000; post_ts='01JAN2024:08:00:01'dt;
    description=repeat('Alpha ',20); code='456'; extra_b='B_only'; output;
  loan_id=2003; customer_id=803; name='Charlie'; amount=2510.00; rate=0.0400000; post_ts='02JAN2024:09:30:00'dt;
    description=repeat('Delta ',30); code='789'; output;
  loan_id=2004; customer_id=804; name='Dane'; amount=3100.00; rate=0.0410000; post_ts='02JAN2024:09:30:00'dt;
    description=catx(' ',repeat('Foxtrot ',28),'EXTRA'); code='111'; output;
  loan_id=2005; customer_id=805; name='EVAN'; amount=1500.00; rate=0.0400000; post_ts='03JAN2024:07:15:04'dt;
    description=catx(' ',repeat('Golf ',35),'DIFF'); code='222'; output;
  loan_id=2007; customer_id=807; name='Ivy'; amount=2300.00; rate=0.039; post_ts='04JAN2024:11:00:00'dt;
    description=repeat('India ',15); code='444'; output;
run;

/* ---- Metadata capture + name normalization (src/enterprise_reconciliation_diagnostic.sas) ---- */
proc contents data=work.A out=work._CA(keep=name type length format formatl formatd) noprint;
run;
proc contents data=work.B out=work._CB(keep=name type length format formatl formatd) noprint;
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

/* ---- Variables present only in one side (A-only vs B-only) via FULL JOIN ---- */
proc sql noprint;
    create table work.RECON_VARS_ONLY_IN_ONE as
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

proc print data=work.RECON_VARS_ONLY_IN_ONE noobs;
    title "Schema drift: variables present in only one dataset (Test 1)";
run;
