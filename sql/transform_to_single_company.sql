-- Transform 5 Companies to 5 Departments of TechCorp Solutions Inc
-- Using actual table structure

BEGIN;

-- Add parent company column
ALTER TABLE companies ADD COLUMN IF NOT EXISTS parent_company VARCHAR(100);

-- Transform companies to departments using existing columns
UPDATE companies SET 
    company_name = CASE company_id
        WHEN 1 THEN 'Engineering Department'
        WHEN 2 THEN 'Product Management Department'  
        WHEN 3 THEN 'Sales & Marketing Department'
        WHEN 4 THEN 'Operations Department'
        WHEN 5 THEN 'Finance & HR Department'
    END,
    industry = 'Technology',
    country = 'United States',
    currency = 'USD',
    founded_year = 2010,
    parent_company = 'TechCorp Solutions Inc';

-- Verify transformation
SELECT 
    company_id,
    company_name as department_name,
    parent_company,
    industry,
    country
FROM companies 
ORDER BY company_id;

COMMIT;

SELECT 'Company structure transformed to departmental model successfully!' as status;

