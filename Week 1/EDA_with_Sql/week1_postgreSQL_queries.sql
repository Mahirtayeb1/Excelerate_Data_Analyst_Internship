create table ApplicantData(
	App_ID Text,
	Country Text,
	University Text,
	Phone_Number Text	
);

copy ApplicantData 
from 'E:\Data Analysis\Excelerate Internship\Week 1\Resource\Csv_files\ApplicantData.csv' 
WITH (FORMAT csv, HEADER true);

select * from ApplicantData
where Length(Phone_Number) > 20;

-- finding missing & null values:
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE App_ID IS NULL OR TRIM(App_ID) = '') AS app_id_nulls,
    COUNT(*) FILTER (WHERE Country IS NULL OR TRIM(Country) = '') AS country_nulls,
    COUNT(*) FILTER (WHERE University IS NULL OR TRIM(University) = '') AS university_nulls,
    COUNT(*) FILTER (WHERE Phone_Number IS NULL OR TRIM(Phone_Number) = '') AS phone_number_nulls
FROM ApplicantData;

Select Count(*) As Total_Duplicates 
From (
	SELECT App_ID, COUNT(*) AS occurrences
	FROM ApplicantData
	GROUP BY App_ID
	HAVING COUNT(*) > 1);




SELECT
    COUNT(*) FILTER (WHERE Phone_Number ~ '[,\/.\-]') AS inconsistent,
	COUNT(*) FILTER (WHERE Length(Phone_Number) > 20) AS Invalid_PhoneNumber,
    COUNT(*) FILTER (WHERE NOT (Phone_Number ~ '[,\/.\-]')) AS clean
FROM ApplicantData;

SELECT
    COUNT(*) FILTER (WHERE App_ID ~ '[,\/.\-]') AS inconsistent_AppID,
    COUNT(*) FILTER (WHERE NOT (App_ID ~ '[,\/.\-]')) AS clean_AppID
FROM ApplicantData;

-- finding top 10 most appeared countries:
SELECT 
    Country, 
    COUNT(*) AS total_appeared
FROM ApplicantData
WHERE Country IS NOT NULL 
	AND TRIM(Country) <> '' 
	AND TRIM(Country) <> '-'
GROUP BY Country
ORDER BY total_appeared DESC
LIMIT 10;



create table OutreachData(
	Reference_ID Text,
	Recieved_At Text,
	University Text,
	Caller_Name Text,
	Outcome_1 Text,
	Remark text,
	Campaign_ID text,
	Escalation_Required Text	
);

copy OutreachData 
from 'E:\Data Analysis\Excelerate Internship\Week 1\Resource\Csv_files\OutreachData.csv' 
WITH (FORMAT csv, HEADER true);

-- finding inconsistent_Reference_ID's from OutreachData
SELECT
    COUNT(*) FILTER (WHERE Reference_ID ~ '[,\/.\-]') AS inconsistent_Reference_ID,
    COUNT(*) FILTER (WHERE NOT (Reference_ID ~ '[,\/.\-]')) AS clean_ReferenceID
FROM OutreachData;


-- creating clean dataset

DROP TABLE IF EXISTS CleanApplicantData;
create table CleanApplicantData(
	App_ID Bigint,
	Country Varchar(56),
	University Varchar(255),
	Phone_Number Text
);

copy CleanApplicantData 
from 'E:\Data Analysis\Excelerate Internship\Week 1\Resource\Cleaned_Csv_files\CleanApplicantData.csv' 
WITH (FORMAT csv, HEADER true);

Select * from CleanApplicantData;


DROP TABLE IF EXISTS Cleaned_CampaignData;
create table Cleaned_CampaignData(
	ID Text,
	Name Varchar(255),
	Category Varchar(56),
	Intake Varchar(20),
	University Varchar(255),
	Status Varchar(20),
	Start_Date timestamp,
	Season Varchar(20),
	Campaign_Type Varchar(20),
	Campaign_Region Varchar(56),
	Campaign_Status Varchar(255)
);

set datestyle to MDY;
copy Cleaned_CampaignData 
from 'E:\Data Analysis\Excelerate Internship\Week 1\Resource\Cleaned_Csv_files\Cleaned_CampaignData.csv' 
WITH (FORMAT csv, HEADER true);

Select * from Cleaned_CampaignData;
Select Count(*) from Cleaned_CampaignData;


DROP TABLE IF EXISTS Cleaned_OutreachData;
create table Cleaned_OutreachData(
	Outreach_ID int,
	Reference_ID Bigint,
	Recieved_At timestamp,
	University varchar(255),
	Caller_Name varchar(20),
	Outcome_1 Text,
	Remark Text,
	Campaign_ID Varchar(20),
	Escalation_Required Varchar(20),
	Date timestamp,
	Year Varchar(10),
	Time Time,
	Hour int,
	Weekday Varchar(20),
	Month Varchar(20)	
);

set datestyle to MDY;
copy Cleaned_OutreachData 
from 'E:\Data Analysis\Excelerate Internship\Week 1\Resource\Cleaned_Csv_files\Cleaned_OutreachData.csv' 
WITH (FORMAT csv, HEADER true);

Select * from Cleaned_OutreachData;
Select Count(*) from Cleaned_OutreachData;

-- checking Cleaned data is actually cleaned or not
-- CleanApplicantData
SELECT
    COUNT(*) FILTER (WHERE Phone_Number ~ '[,\/.\-]') AS inconsistent,
	COUNT(*) FILTER (WHERE Length(Phone_Number) > 20) AS Invalid_PhoneNumber,
    COUNT(*) FILTER (WHERE NOT (Phone_Number ~ '[,\/.\-]')) AS clean
FROM CleanApplicantData;

SELECT
    COUNT(*) FILTER (WHERE App_ID::TEXT ~ '^[0-9]+$') AS numeric_only,
    COUNT(*) FILTER (WHERE NOT (App_ID::TEXT ~ '^[0-9]+$')) AS non_numeric
FROM CleanApplicantData;

Select Distinct Count(ID) from Cleaned_CampaignData;
SELECT COUNT(DISTINCT Season) AS distinct_seasons
FROM Cleaned_CampaignData;

SELECT Season, COUNT(*) AS total_rows
FROM Cleaned_CampaignData
GROUP BY Season
ORDER BY total_rows DESC;

-- Exploring Cleaned_OutreachData
SELECT Caller_Name, COUNT(*) AS total_callers
FROM Cleaned_OutreachData
GROUP BY Caller_Name
ORDER BY total_callers DESC;

Select Distinct Count(Reference_ID) As Total_Applicants
from Cleaned_OutreachData;

-- Creating Master Table:

SELECT *
FROM CleanApplicantData a
FULL OUTER JOIN Cleaned_OutreachData o
    ON a.App_ID = o.Reference_ID;

CREATE TABLE MasterTable AS
SELECT
    a.App_ID,
    a.Country,
    a.University,
    a.Phone_Number,
    c.ID AS Campaign_ID,
    c.Name AS Campaign_Name,
    c.Season,
    c.Campaign_Type,
    c.Campaign_Region,
    o.Outreach_ID,
    o.Recieved_At,
    o.Outcome_1,
    o.Remark
FROM CleanApplicantData a
FULL OUTER JOIN Cleaned_OutreachData o 
    ON a.App_ID = o.Reference_ID
FULL OUTER JOIN Cleaned_CampaignData c 
    ON o.Campaign_ID = c.ID;   

SELECT * 
FROM MasterTable
LIMIT 10;

SELECT
    COUNT(*) FILTER (WHERE App_ID IS NOT NULL 
                     AND Outreach_ID IS NOT NULL 
                     AND Campaign_ID IS NOT NULL) AS full_linked,
    COUNT(*) FILTER (WHERE App_ID IS NOT NULL 
                     AND Outreach_ID IS NULL 
                     AND Campaign_ID IS NULL) AS applicants_only,
    COUNT(*) FILTER (WHERE App_ID IS NULL 
                     AND Outreach_ID IS NOT NULL 
                     AND Campaign_ID IS NULL) AS outreach_only,
    COUNT(*) FILTER (WHERE App_ID IS NULL 
                     AND Outreach_ID IS NULL 
                     AND Campaign_ID IS NOT NULL) AS campaigns_only
FROM MasterTable;

-- 16 applicants who are Orphan or not connected with master table
SELECT 
    App_ID,
    Country,
    University,
    Phone_Number
FROM MasterTable
WHERE Outreach_ID IS NULL
  AND Campaign_ID IS NULL
  AND App_ID IS NOT NULL;

