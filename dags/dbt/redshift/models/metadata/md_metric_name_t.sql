{{ config(
    materialized="table",
    on_schema_change="append_new_columns",
    alias="md_metric_name_t",
    tags=["metadata"]
) }}

SELECT 'brcs1' AS attribute, 'BRCS1 - Incorporation date (>24 months)' AS value, 14 AS summary_order
UNION ALL
SELECT 'brcs1_1', 'BRCS1.1 - Incorporation date (>6 months)', 15
UNION ALL
SELECT 'brcs6', 'BRCS6 - Winding up suit (no recently concluded winding up suits - 24 months)', 21
UNION ALL
SELECT 'bs1', 'BS1 - Large Transfer to individual parties', 38
UNION ALL
SELECT 'bs3', '', NULL
UNION ALL
SELECT 'bri_not_found', 'BRI Not Found', 13
UNION ALL
SELECT 'brcs16', 'BRCS16 - PGs need to take up more than 70% shareholding', 29
UNION ALL
SELECT 'brcs5', 'BRCS5 - Winding up suit (no pending winding up suits)', 20
UNION ALL
SELECT 'brcs7', '', NULL
UNION ALL
SELECT 'bri4', 'BRI4 - PGs where there has been a search for “Debt Collection” in the last 24 months', 33
UNION ALL
SELECT 'bs7', 'BS7 - Minimally of 2 different Transaction Categories', 43
UNION ALL
SELECT 'bs9', '', NULL
UNION ALL
SELECT 'cbs10', '', NULL
UNION ALL
SELECT 'cbs7_1', '', NULL
UNION ALL
SELECT 'fid1_2', '', NULL
UNION ALL
SELECT 'written_off', 'No of Closed - Written Off', 6
UNION ALL
SELECT 'brcs15', 'BRCS15 - SME where there has been a search for “Debt Collection” in the last 24 months', 28
UNION ALL
SELECT 'brcs18', '', NULL
UNION ALL
SELECT 'brcs3_1', 'BRCS3.1 - Paid up capital (>SG1k)', 18
UNION ALL
SELECT 'bs6', 'BS6 - Large cash withdrawal', 42
UNION ALL
SELECT 'cbs2', 'CBS2 - No R (restructured loans), W (written off), S (partial settlement) status', 35
UNION ALL
SELECT 'cbs9', '', NULL
UNION ALL
SELECT 'n_approval', 'Approve', NULL
UNION ALL
SELECT 'brcs19', '', NULL
UNION ALL
SELECT 'brcs4', 'BRCS4 - Nationality of PG (no EP/SP, only SG or PR), min 1', 19
UNION ALL
SELECT 'bs2', 'BS2 - Large Transfer to related companies', 39
UNION ALL
SELECT 'cbs3_2', 'CBS3.2 - No "D" status in the L12M and account is open', 36
UNION ALL
SELECT 'cbs6', '', NULL
UNION ALL
SELECT 'neg_limit', '', NULL
UNION ALL
SELECT 'brcs2', 'BRCS2 - Status (whether it is a live company)', 16
UNION ALL
SELECT 'brcs20', '', NULL
UNION ALL
SELECT 'bri1', 'BRI1 - Bankruptcy suit', 30
UNION ALL
SELECT 'cbs7_2', '', NULL
UNION ALL
SELECT 'cbs8', '', NULL
UNION ALL
SELECT 'rg_4_6', 'RG - Risk Grade 4-6', NULL
UNION ALL
SELECT 'bank_statement_fail', 'Bank Statement Failed', 10
UNION ALL
SELECT 'brcs13', 'BRCS13 - Minimum personal shareholding must be > 75%', 26
UNION ALL
SELECT 'bs4', 'BS4 - Intra-company Transactions', 40
UNION ALL
SELECT 'bs5', 'BS5 - More than 2 outward cheque bounces', 41
UNION ALL
SELECT 'bs8', '', NULL
UNION ALL
SELECT 'n_cases', 'No of Cases', 1
UNION ALL
SELECT 'rg_1_3', 'RG - Risk Grade 1-3', NULL
UNION ALL
SELECT 'rg_7_8', 'RG - Risk Grade 7-8', NULL
UNION ALL
SELECT 'n_reject', 'Rejected', NULL
UNION ALL
SELECT 'bad_standing', 'No of Active - Bad Standing', 4
UNION ALL
SELECT 'obligation_met', 'No of Closed - Obligations Met', 5
UNION ALL
SELECT 'brcs11', 'BRCS11 - UEN/NRIC (match hit against our blacklist)', 24
UNION ALL
SELECT 'brcs12', 'BRCS12 - Address of the SME in the ACRA is same as the Address of the Director in the BRIG', 25
UNION ALL
SELECT 'brcs17', '', NULL
UNION ALL
SELECT 'brcs3', 'BRCS3 - Paid up capital (>SG20k)', 17
UNION ALL
SELECT 'brcs8', '', NULL
UNION ALL
SELECT 'bri5', '', NULL
UNION ALL
SELECT 'fid1_1', '', NULL
UNION ALL
SELECT 'rg_9_10', 'RG - Risk Grade 9-10', 44
UNION ALL
SELECT 'hard_rule_fail', 'Hard Rule Failed', 11
UNION ALL
SELECT 'good_standing', 'No of Active - Good Standing', 3
UNION ALL
SELECT 'brcs10', 'BRCS10 - Credit suit (pending) - company', 23
UNION ALL
SELECT 'brcs14', 'BRCS14 - Cannot have Foreign Director/shareholders with > 25% shareholding', 27
UNION ALL
SELECT 'brcs9', 'BRCS9 - Type of company (no sole proprietorship or partnership or LLP)', 22
UNION ALL
SELECT 'bri2', 'BRI2 - Negative/Blacklist (no negative/blacklist hit)', 31
UNION ALL
SELECT 'bri3', 'BRI3 - Credit suit (pending) - individual', 32
UNION ALL
SELECT 'bri6', '', NULL
UNION ALL
SELECT 'bri7', '', NULL
UNION ALL
SELECT 'cbs1', 'CBS1 - No default records', 34
UNION ALL
SELECT 'cbs4', 'CBS4 - Debt Management Program', 37
UNION ALL
SELECT 'fid2', '', NULL
UNION ALL
SELECT 'n_disbursed', 'No of Loan IDs Disbursed', 2
UNION ALL
SELECT 'fraud_rule_fail', 'Fraud Rule Failed', 12