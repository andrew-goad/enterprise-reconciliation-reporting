# Enterprise Dataset Reconciliation & ML Pattern Mining

## 🎯 Strategic Intent: Audit-Ready Transparency
**How do you ensure data integrity across enterprise systems while providing audit-ready transparency?**

I developed this robust SAS macro to reconcile complex datasets with high precision. It features **tolerance-aware checks** for numeric and temporal data alongside **ML pattern mining** to identify frequent co-occurrences in data differences. This framework bridges the gap between technical auditing and business governance, ensuring 100% transparency for stakeholders.

---

### 📈 Executive "Talk Tracks"
* **Beyond the "Diff":** Most tools tell you *that* data is different; this engine tells you *why*. By mining patterns in discrepancies, we can identify systemic ETL failures or logic drifts.
* **Tolerance-Aware Logic:** We eliminate "false positives" by setting business-defined thresholds for currency and date rounding, allowing auditors to focus only on material risks.
* **Governance Bridge:** This isn't just a technical log—it's a governance tool. It translates complex table comparisons into executive-ready narratives that prove the integrity of the data supply chain.
* **The "No Cold Handoffs" Promise:** Every reconciliation produces a forensic audit trail, ensuring that the results are defendable to internal regulators or external oversight bodies.

---

### 🛠️ Technical Rigor & Architecture
* **Metadata-Driven Mapping:** Leverages `SAS Dictionary` tables to automatically align disparate schemas between "System of Record" and "Downstream Warehouse."
* **ML Pattern Analysis:** Custom logic to calculate the frequency of co-occurring differences (e.g., "When Variable A is wrong, Variable B is also wrong 90% of the time").
* **Dynamic Macro Arrays:** Extensive use of macro variables and `SYMGET` to handle variable-length datasets without hard-coding field names.
* **Diagnostic Packaging:** Automated generation of "Discrepancy Profiles" that categorize errors by type, severity, and remediation priority.

---

### 🛡️ Integrity & Confidentiality Note
**Data Privacy:** This repository demonstrates the forensic methodology for enterprise reconciliation. No proprietary bank data or specific system schemas are included; all comparisons are performed on synthetic datasets.

---
**Philosophy:** “No Cold Handoffs”—engineering zero-defect, audit-ready results.
