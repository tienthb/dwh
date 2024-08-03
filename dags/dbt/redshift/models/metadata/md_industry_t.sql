{{ config(
    materialized="table",
    on_schema_change="append_new_columns",
    alias="md_industry_t",
    tags=["metadata"]
) }}

SELECT 'Accommodation' AS industry, 'Accommodation and Food Service Activities' AS industry_group
UNION ALL
SELECT 'Accommodation and Food Service Activities', 'Accommodation and Food Service Activities' 
UNION ALL
SELECT 'Administrative & Support Services', 'Administrative & Support Services' 
UNION ALL
SELECT 'Administrative & Support Services Activities', 'Administrative & Support Services' 
UNION ALL
SELECT 'Administrative and Support Service Activities', 'Administrative & Support Services' 
UNION ALL
SELECT 'Agriculture and Fishing', 'Agriculture and Fishing' 
UNION ALL
SELECT 'Construction', 'Construction' 
UNION ALL
SELECT 'Education', 'Education' 
UNION ALL
SELECT 'Electricity, Gas, Steam and Air-Conditioning Supply', 'Electricity, Gas, Steam and Air-Conditioning Supply' 
UNION ALL
SELECT 'F&B', 'Food & Beverages' 
UNION ALL
SELECT 'FnB', 'Food & Beverages' 
UNION ALL
SELECT 'Food & Beverages', 'Food & Beverages' 
UNION ALL
SELECT 'Information & Communication', 'Information & Communication' 
UNION ALL
SELECT 'Information & Communications', 'Information & Communication' 
UNION ALL
SELECT 'Information and Communications', 'Information & Communication' 
UNION ALL
SELECT 'Manufacturing', 'Manufacturing' 
UNION ALL
SELECT 'Marine', 'Marine' 
UNION ALL
SELECT 'Mining and Quarrying', 'Mining and Quarrying' 
UNION ALL
SELECT 'Others', 'Others' 
UNION ALL
SELECT 'Professional, Scientific & Technical Services', 'Professional, Scientific & Technical Services' 
UNION ALL
SELECT 'Professional, Scientific and Technical Activities', 'Professional, Scientific & Technical Services' 
UNION ALL
SELECT 'Public Administration and Defence', 'Public Administration and Defence' 
UNION ALL
SELECT 'Recreation, Community & Personal Services', 'Recreation, Community & Personal Services' 
UNION ALL
SELECT 'Retail', 'Wholesale & Retail Trade' 
UNION ALL
SELECT 'Transportation', 'Transportation & Storage' 
UNION ALL
SELECT 'Transportation and Storage', 'Transportation & Storage' 
UNION ALL
SELECT 'Utility & Water Supply & Waste Management', 'Utility, Water Supply & Waste Management' 
UNION ALL
SELECT 'Utility, Water Supply & Waste Management', 'Utility, Water Supply & Waste Management' 
UNION ALL
SELECT 'Water Supply; Sewerage, Waste Management and Remediation Activities', 'Utility, Water Supply & Waste Management' 
UNION ALL
SELECT 'Wholesale', 'Wholesale & Retail Trade' 
UNION ALL
SELECT 'Wholesale and Retail Trade', 'Wholesale & Retail Trade' 
UNION ALL 
SELECT 'Missing', 'Missing'