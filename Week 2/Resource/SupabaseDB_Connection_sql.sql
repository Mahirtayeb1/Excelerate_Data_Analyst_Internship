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
