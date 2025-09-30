SELECT * FROM "CleanApplicantData"
WHERE "Country" = 'India';

SELECT * FROM "Cleaned_CampaignData";

select * from "Cleaned_OutreachData";

DROP MATERIALIZED VIEW IF EXISTS master_table;
CREATE MATERIALIZED VIEW master_table AS
SELECT 
    -- o."Outreach_ID",
    o."Reference_ID" AS app_id,
    a."Country",
    a."University" AS Applicant_University,
    a."Phone_Number",
    o."Recieved_At",
    o."Caller_Name",
    o."Outcome_1",
    o."Remark",
    o."Campaign_ID",
    c."Name" AS Campaign_Name,
    c."Category" AS Campaign_Category,
	c."Season" AS Campaign_Season,
    c."Campaign_Type",
    c."Campaign_Region",
    c."Status" AS Campaign_Status,
    c."Status_Category",
    c."Deposit_Status",
    c."Completion_Status",
    c."Start_Date" AS Campaign_Start_Date,
    o."Date" AS Call_Date,
    o."Time" AS Call_Time,
    o."Hour" AS Call_Hour,
    o."Weekday" AS Call_Weekday,
    o."Month" AS Call_Month
FROM "Cleaned_OutreachData" o
LEFT JOIN "CleanApplicantData" a 
    ON o."Reference_ID" = a."App_ID"
LEFT JOIN "Cleaned_CampaignData" c 
    ON o."Campaign_ID" = c."ID";


select * from master_table;




-- Week 3: Data Validation

Select * from "Cleaned_CampaignData"
where "ID" = 'AANF23';

-- 1. Data Consistency Check

-- Datatype of App_ID
SELECT pg_typeof("App_ID") AS app_id_type
FROM "CleanApplicantData"
LIMIT 1;

-- Datatype of Reference_ID of Cleaned_OutreachData
SELECT pg_typeof("Reference_ID") AS app_id_type
FROM "Cleaned_OutreachData"
LIMIT 1;

-- Finding rows where App_ID is null or non-numeric
SELECT COUNT(*) AS null_ids
FROM "CleanApplicantData"
WHERE "App_ID" IS NULL;

-- Checking texts in App_ID
SELECT COUNT(*) 
FROM "CleanApplicantData"
WHERE CAST("App_ID" AS TEXT) !~ '^[0-9]+$';

-- Checking duplicates in CleanApplicantData
SELECT "App_ID", COUNT(*) AS occurrences
FROM "CleanApplicantData"
GROUP BY "App_ID"
HAVING COUNT(*) > 1;

-- ApplicantData: Phone_Number should be numeric
SELECT COUNT(*) FILTER (WHERE "Phone_Number" ~ '^[0-9]+$') AS numeric_phones,
       COUNT(*) FILTER (WHERE "Phone_Number" !~ '^[0-9]+$') AS invalid_phones
FROM "CleanApplicantData";


-- CampaignData: Check if Start_Date is valid timestamp range
SELECT MIN("Start_Date") AS earliest, MAX("Start_Date") AS latest
FROM "Cleaned_CampaignData";

-- OutreachData: Check if Recieved_At contains valid timestamps
SELECT MIN("Recieved_At") AS earliest, MAX("Recieved_At") AS latest
FROM "Cleaned_OutreachData";

-- Correlation Between Key Identifiers
-- Outreach Reference_ID should match ApplicantData.App_ID
SELECT COUNT(*) AS missing_applicants
FROM "Cleaned_OutreachData" o
LEFT JOIN "CleanApplicantData" a ON o."Reference_ID" = a."App_ID"
WHERE a."App_ID" IS NULL;

-- how many applicant ID not null
SELECT COUNT(*) AS valid_applicants
FROM "Cleaned_OutreachData" o
LEFT JOIN "CleanApplicantData" a ON o."Reference_ID" = a."App_ID"
WHERE a."App_ID" IS not NULL;

-- checking other informations of those null app_id's
SELECT "App_ID", "Country", "Phone_Number"
FROM "Cleaned_OutreachData" o
LEFT JOIN "CleanApplicantData" a ON o."Reference_ID" = a."App_ID"
WHERE a."App_ID" IS NULL;

-- Outreach Campaign_ID should match CampaignData.ID
SELECT COUNT(*) AS missing_campaigns
FROM "Cleaned_OutreachData" o
LEFT JOIN "Cleaned_CampaignData" c ON o."Campaign_ID" = c."ID"
WHERE c."ID" IS NULL;

-- Reference_Id of those null app_id
SELECT DISTINCT o."Reference_ID"
FROM "Cleaned_OutreachData" o
LEFT JOIN "CleanApplicantData" a ON o."Reference_ID" = a."App_ID"
WHERE a."App_ID" IS NULL;

-- 2. completeness verification

-- ApplicantData missing values
SELECT 
    COUNT(*) FILTER (WHERE "Country" IS NULL OR "Country" = '') AS missing_country,
    COUNT(*) FILTER (WHERE "University" IS NULL OR "University" = '') AS missing_university,
    COUNT(*) FILTER (WHERE "Phone_Number" IS NULL OR "Phone_Number" = '') AS missing_phone
FROM "CleanApplicantData";

-- CampaignData missing values
SELECT 
    COUNT(*) FILTER (WHERE "Name" IS NULL OR "Name" = '') AS missing_name,
    COUNT(*) FILTER (WHERE "Start_Date" IS NULL) AS missing_start_date,
    COUNT(*) FILTER (WHERE "Campaign_Type" IS NULL OR "Campaign_Type" = '') AS missing_campaign_type
FROM "Cleaned_CampaignData";

-- OutreachData missing values
SELECT 
    COUNT(*) FILTER (WHERE "University" IS NULL OR "University" = '') AS missing_university,
    COUNT(*) FILTER (WHERE "Caller_Name" IS NULL OR "Caller_Name" = '') AS missing_caller,
    COUNT(*) FILTER (WHERE "Outcome_1" IS NULL OR "Outcome_1" = '') AS missing_outcome
FROM "Cleaned_OutreachData";


-- 3. Duplicates detection

-- Checking duplicates in CleanApplicantData
SELECT "App_ID", COUNT(*) AS occurrences
FROM "CleanApplicantData"
GROUP BY "App_ID"
HAVING COUNT(*) > 1;

-- Duplicates in Cleaned_CampaignData's IDs
SELECT "ID", COUNT(*) 
FROM "Cleaned_CampaignData"
GROUP BY "ID"
HAVING COUNT(*) > 1;

-- Duplicates in Cleaned_OutreachData's IDs
SELECT "Reference_ID", COUNT(*) as total_Duplicates
FROM "Cleaned_OutreachData"
GROUP BY "Reference_ID"
HAVING COUNT(*) > 1
Order by total_Duplicates desc;

SELECT 
    "Reference_ID", "Campaign_ID", "Recieved_At", "Caller_Name", "Outcome_1", COUNT(*) AS dup_count
FROM "Cleaned_OutreachData"
GROUP BY "Reference_ID", "Campaign_ID", "Recieved_At", "Caller_Name", "Outcome_1"
HAVING COUNT(*) > 1
ORDER BY dup_count DESC;

SELECT 
    "Reference_ID", "Campaign_ID", "Recieved_At", "Outcome_1", COUNT(*) AS dup_count
FROM "Cleaned_OutreachData"
GROUP BY "Reference_ID", "Campaign_ID", "Recieved_At", "Outcome_1"
HAVING COUNT(*) > 1;

-- Total calls including duplicates
SELECT COUNT(*) AS total_calls FROM "Cleaned_OutreachData";

-- Unique calls (ignoring duplicates based on key fields)
SELECT COUNT(DISTINCT "Reference_ID" || '-' || "Campaign_ID" || '-' || "Recieved_At" || '-' || "Outcome_1") AS unique_calls
FROM "Cleaned_OutreachData";


-- 4. Business Rule Validation

SELECT
    COUNT(*) AS total_calls,
    COUNT(*) FILTER (WHERE "Outcome_1" NOT IN ('Not Connected', 'Disconnected')) AS connected_calls,
    COUNT(*) FILTER (WHERE "Outcome_1" IN ('Not Connected', 'Disconnected')) AS disconnected_calls
FROM "Cleaned_OutreachData";


SELECT "ID", "Name", "Intake", "Start_Date"
FROM "Cleaned_CampaignData"
WHERE ("Intake" LIKE '%24' AND EXTRACT(YEAR FROM "Start_Date") <> 2024)
   OR ("Intake" LIKE '%25' AND EXTRACT(YEAR FROM "Start_Date") <> 2025);

SELECT "ID", "Name", "Intake", "Start_Date",
       CASE 
           WHEN "Intake" LIKE 'AY%' THEN 2024
           WHEN "Intake" LIKE 'FA24%' THEN 2024
           WHEN "Intake" LIKE 'SP25%' THEN 2025
           ELSE NULL
       END AS expected_year
FROM "Cleaned_CampaignData"
WHERE EXTRACT(YEAR FROM "Start_Date") <> 
      CASE 
           WHEN "Intake" LIKE 'AY%' THEN 2024
           WHEN "Intake" LIKE 'FA24%' THEN 2024
           WHEN "Intake" LIKE 'SP25%' THEN 2025
           ELSE EXTRACT(YEAR FROM "Start_Date")
      END;


Select Distinct "Outcome_1"
from "Cleaned_OutreachData";

SELECT o."Outreach_ID", o."Campaign_ID", c."Intake", c."Start_Date" AS campaign_start,
       o."Recieved_At" AS outreach_date
FROM "Cleaned_OutreachData" o
JOIN "Cleaned_CampaignData" c
ON o."Campaign_ID" = c."ID"
WHERE o."Recieved_At" < c."Start_Date"  -- before campaign start
   OR o."Recieved_At" > 
      CASE 
          WHEN c."Intake" LIKE '%24%' THEN '2024-12-31'::date
          WHEN c."Intake" LIKE '%25%' THEN '2025-12-31'::date
          ELSE o."Recieved_At"
      END;
SELECT 
    c."Intake",
    COUNT(*) AS total_outreach,
    COUNT(*) FILTER (
        WHERE o."Recieved_At" < c."Start_Date"
           OR o."Recieved_At" > 
              CASE 
                  WHEN c."Intake" LIKE '%24%' THEN '2024-12-31'::date
                  WHEN c."Intake" LIKE '%25%' THEN '2025-12-31'::date
                  ELSE o."Recieved_At"
              END
    ) AS invalid_outreach
FROM "Cleaned_OutreachData" o
JOIN "Cleaned_CampaignData" c
ON o."Campaign_ID" = c."ID"
GROUP BY c."Intake";

-- Validation of Campaign and Outreach Timeframes:
SELECT o."Outreach_ID", o."Campaign_ID", c."Intake", c."Start_Date" AS campaign_start,
       o."Recieved_At" AS outreach_date
FROM "Cleaned_OutreachData" o
JOIN "Cleaned_CampaignData" c
ON o."Campaign_ID" = c."ID"
WHERE o."Recieved_At" < c."Start_Date"
   OR o."Recieved_At" > 
      CASE 
          WHEN c."Intake" LIKE '%24%' THEN '2024-12-31'::date
          WHEN c."Intake" LIKE '%25%' THEN '2025-12-31'::date
          ELSE o."Recieved_At"
      END;



