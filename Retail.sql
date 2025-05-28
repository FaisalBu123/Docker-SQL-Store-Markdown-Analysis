# https://www.kaggle.com/datasets/manjeetsingh/retaildataset/data
# This contains historical data from 45 stores 
# I will only analyse one of the csv files('Features data set') that I find the most interesting
# the other 2 can be done later on especially for the modelling part which could be done using Python as will be explained later on
# The docker instructions I used:
	
# 1. Make a new directory 
# mkdir data-science-db
	
# 2. Change into directory
# cd data-science-db

# 3. Pull the latest PostgreSQL image
# docker pull postgres

# 4. Run the PostgreSQL container with a password
# docker run --name salesSQL-postgres -e POSTGRES_PASSWORD=secret -d postgres

# 5. Create a new PostgreSQL database inside  container
# docker exec -u postgres salesSQL-postgres createdb postgres_db

# 6. Connect to database using psql
# docker exec -it salesSQL-postgres psql -U postgres -d postgres_db



SELECT * 
FROM features;

# 7605 rows

# First thing we want to do is create a copy table. This is the one we will work in and clean the data in. We want a table with the original raw data in case something happens
CREATE TABLE feat_copy 
LIKE features;

INSERT feat_copy
SELECT * FROM features;

-- Now when we are data cleaning we usually follow a few steps:
-- 1. check for duplicates and remove any
-- 2. standardise data and fix errors
-- 3. Look at null values and deal with them
-- 4. remove any columns and rows that are not necessary 

SELECT *
FROM feat_copy
;

# 1.Let us remove duplicates

SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY Store, `Date`, Temperature, Fuel_Price, MarkDown1, MarkDown2, MarkDown3, MarkDown4, MarkDown5, CPI, Unemployment, IsHoliday) AS row_num
	FROM 
		feat_copy;
        
        
# Note that Date is surrounded by backticks because they are reserved keywords in MySQL.

SELECT *
FROM (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY Store, `Date`, Temperature, Fuel_Price, MarkDown1, MarkDown2, MarkDown3, MarkDown4, MarkDown5, CPI, Unemployment, IsHoliday
			) AS row_num
	FROM 
		feat_copy
) duplicates
WHERE 
	row_num > 1;
    
# No rows returned, fantastic, means no duplicates

# 2. Standardise and fix errors

SELECT *
FROM feat_copy
;

# do distinct viewing of the columns so that we can see if some of them have two labels, have spaces before and after, etc

# let us look at the Store variable
SELECT DISTINCT Store
FROM feat_copy
ORDER BY Store;
# No problems here

# Before inspecting date variable let us convert to correct format:

-- we can use str to date to update this field
UPDATE feat_copy
SET `Date` = STR_TO_DATE(`Date`, '%d/%m/%Y');


SELECT DISTINCT `Date`
FROM feat_copy
ORDER BY `Date`;

ALTER TABLE feat_copy
MODIFY COLUMN `Date` DATE;
# The date variable looks good
# No problems here
# All variable spacing seems correct


# Let us inspect IsHoliday 

SELECT DISTINCT IsHoliday
FROM feat_copy
ORDER BY IsHoliday;
# no problems

# 3. Let us check null values

SELECT *
FROM feat_copy
;
# Markdown cells have missing values but let us inspect in detail

SELECT *
FROM feat_copy
WHERE Store IS NULL 
   OR Store = '' 
   OR Store = 'NA'
ORDER BY Store;
# No missing

# I am inspecting variable by variable for more specifity 
SELECT *
FROM feat_copy
WHERE `Date` IS NULL 
   OR `Date` = '' 
   OR `Date`  = 'NA'
ORDER BY `Date`;
# No missing

SELECT *
FROM feat_copy
WHERE Fuel_Price IS NULL 
   OR Fuel_Price = '' 
   OR Fuel_Price  = 'NA'
ORDER BY Fuel_Price;
# No missing

SELECT *
FROM feat_copy
WHERE MarkDown1 IS NULL 
   OR MarkDown1 = '' 
   OR MarkDown1  = 'NA'
ORDER BY MarkDown1;
# Missing values here

SELECT *
FROM feat_copy
WHERE MarkDown2 IS NULL 
   OR MarkDown2 = '' 
   OR MarkDown2  = 'NA'
ORDER BY MarkDown2;
# Missing values here

SELECT *
FROM feat_copy
WHERE MarkDown3 IS NULL 
   OR MarkDown3 = '' 
   OR MarkDown3  = 'NA'
ORDER BY MarkDown3;
# Missing values here

SELECT *
FROM feat_copy
WHERE MarkDown4 IS NULL 
   OR MarkDown4 = '' 
   OR MarkDown4  = 'NA'
ORDER BY MarkDown4;
# Missing values here

SELECT *
FROM feat_copy
WHERE MarkDown5 IS NULL 
   OR MarkDown5 = '' 
   OR MarkDown5  = 'NA'
ORDER BY MarkDown5;
# Missing values here


SELECT *
FROM feat_copy
WHERE CPI IS NULL 
   OR CPI = '' 
   OR CPI  = 'NA'
ORDER BY CPI;
# Missing values here

SELECT *
FROM feat_copy
WHERE Unemployment IS NULL 
   OR Unemployment = '' 
   OR Unemployment  = 'NA'
ORDER BY Unemployment;
# No missing values here

SELECT *
FROM feat_copy
WHERE IsHoliday IS NULL 
   OR IsHoliday = '' 
   OR IsHoliday  = 'NA'
ORDER BY IsHoliday;
# No missing values here

# So only the Markdowns. The dataset on kaggle has a description that says markdowns are not available pre Nov 2011 so this explained some of this
# To deal with them we will impute the with 0, meaning no markdowns, taking inspiration from:
# https://medium.com/analytics-vidhya/how-to-use-historical-markdown-data-to-predict-store-sales-f670af542033


UPDATE feat_copy
SET 
  MarkDown1 = CASE WHEN MarkDown1 = 'NA' THEN 0 ELSE MarkDown1 END,
  MarkDown2 = CASE WHEN MarkDown2 = 'NA' THEN 0 ELSE MarkDown2 END,
  MarkDown3 = CASE WHEN MarkDown3 = 'NA' THEN 0 ELSE MarkDown3 END,
  MarkDown4 = CASE WHEN MarkDown4 = 'NA' THEN 0 ELSE MarkDown4 END,
  MarkDown5 = CASE WHEN MarkDown5 = 'NA' THEN 0 ELSE MarkDown5 END;


SELECT *
FROM feat_copy
;
# Looks good now.



-- 4. remove any columns and rows that are not necessary 

# There are rows with all markdowns missing. It could be a good idea to drop them. But, since they are documented and expected as mentioned before, then they should be kept.
# Also, removing the rows depends on the other variables in these rows and what our overall analysis is. There are other factors in these rows that are important such as IsHoliday that our important for our analysis so removing these rows would be a bad decision


# Describe() and summary() functions otside of MySQL show statistical summaries, here I will try to replicate them on MySQL

SELECT  
  COUNT(MarkDown1) AS count,
  MIN(MarkDown1) AS min,
  MAX(MarkDown1) AS max,
  AVG(MarkDown1) AS mean,
  STDDEV(MarkDown1) AS std_dev,
  VARIANCE(MarkDown1) AS variance
FROM feat_copy;
# This shows that there is some negative markdowns
# that is wrong because markdowns are the decrease in the original product price, and they should not be negative

SELECT  
  COUNT(MarkDown2) AS count,
  MIN(MarkDown2) AS min,
  MAX(MarkDown2) AS max,
  AVG(MarkDown2) AS mean,
  STDDEV(MarkDown2) AS std_dev,
  VARIANCE(MarkDown2) AS variance
FROM feat_copy;
# Also has negative values

SELECT  
  COUNT(MarkDown3) AS count,
  MIN(MarkDown3) AS min,
  MAX(MarkDown3) AS max,
  AVG(MarkDown3) AS mean,
  STDDEV(MarkDown3) AS std_dev,
  VARIANCE(MarkDown3) AS variance
FROM feat_copy;
# Also has negative values

SELECT  
  COUNT(MarkDown4) AS count,
  MIN(MarkDown4) AS min,
  MAX(MarkDown4) AS max,
  AVG(MarkDown4) AS mean,
  STDDEV(MarkDown4) AS std_dev,
  VARIANCE(MarkDown4) AS variance
FROM feat_copy;
# good

SELECT  
  COUNT(MarkDown5) AS count,
  MIN(MarkDown5) AS min,
  MAX(MarkDown5) AS max,
  AVG(MarkDown5) AS mean,
  STDDEV(MarkDown5) AS std_dev,
  VARIANCE(MarkDown5) AS variance
FROM feat_copy;
# Also has negative values


SELECT  
  COUNT(Temperature) AS count,
  MIN(Temperature) AS min,
  MAX(Temperature) AS max,
  AVG(Temperature) AS mean,
  STDDEV(Temperature) AS std_dev,
  VARIANCE(Temperature) AS variance
FROM feat_copy;
# Looks good


SELECT  
  COUNT(Fuel_price) AS count,
  MIN(Fuel_price) AS min,
  MAX(Fuel_price) AS max,
  AVG(Fuel_price) AS mean,
  STDDEV(Fuel_price) AS std_dev,
  VARIANCE(Fuel_price) AS variance
FROM feat_copy;
# Looks good

SELECT  
  COUNT(CPI) AS count,
  MIN(CPI) AS min,
  MAX(CPI) AS max,
  AVG(CPI) AS mean,
  STDDEV(CPI) AS std_dev,
  VARIANCE(CPI) AS variance
FROM feat_copy;
# Looks good

SELECT  
  COUNT(Unemployment) AS count,
  MIN(Unemployment) AS min,
  MAX(Unemployment) AS max,
  AVG(Unemployment) AS mean,
  STDDEV(Unemployment) AS std_dev,
  VARIANCE(Unemployment) AS variance
FROM feat_copy;
# Looks good

# So let us deal with the markdowns:

# Using Case When to count total number of negative values for markdowns
SELECT
  SUM(CASE WHEN MarkDown1 < 0 THEN 1 ELSE 0 END) AS MarkDown1_neg,
  SUM(CASE WHEN MarkDown2 < 0 THEN 1 ELSE 0 END) AS MarkDown2_neg,
  SUM(CASE WHEN MarkDown3 < 0 THEN 1 ELSE 0 END) AS MarkDown3_neg,
  SUM(CASE WHEN MarkDown5 < 0 THEN 1 ELSE 0 END) AS MarkDown5_neg,
  (
    SUM(CASE WHEN MarkDown1 < 0 THEN 1 ELSE 0 END) +  # Multiple lines so keep bracketed
    SUM(CASE WHEN MarkDown2 < 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN MarkDown3 < 0 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN MarkDown5 < 0 THEN 1 ELSE 0 END)
  ) AS total_negative_values
FROM feat_copy;
# 43 toal negative. Low number. Let us convert them to 0.

UPDATE feat_copy
SET 
  MarkDown1 = CASE WHEN MarkDown1 < 0 THEN 0 ELSE MarkDown1 END,
  MarkDown2 = CASE WHEN MarkDown2 < 0 THEN 0 ELSE MarkDown2 END,
  MarkDown3 = CASE WHEN MarkDown3 < 0 THEN 0 ELSE MarkDown3 END,
  MarkDown4 = CASE WHEN MarkDown4 < 0 THEN 0 ELSE MarkDown4 END,
  MarkDown5 = CASE WHEN MarkDown5 < 0 THEN 0 ELSE MarkDown5 END;


# So, now we are done with the data cleaning and can start with the EDA

-- EDA

-- explore the data and find trends or patterns

SELECT * 
FROM feat_copy;


SELECT AVG(MarkDown1) as avg_md1,AVG(MarkDown2) as avg_md2, AVG(MarkDown3) as avg_md3, AVG(MarkDown4) as avg_md4, AVG(MarkDown5)  as avg_md5
FROM feat_copy;
# Highest avg markdowns 1 and 5 and lowest 3
#Highest average values for MarkDown1 and MarkDown5, and lowest for MarkDown3
# Note that these values are including non holiday days too (where the markdowns are generally much less) so grouping by holiday later on in the analysis would be useful


# Let us visualise the top 15 average CPI's and temps by store
# Base CPI is 100
SELECT Store, AVG(CPI) AS avg_cpi, COUNT(*) AS records, AVG(Temperature) as avg_temp
FROM feat_copy
GROUP BY Store
ORDER BY avg_cpi DESC
LIMIT 15;
# SQL first starts with feat_copy then groups by store, THEN it does the averages and count. Just interesting to note the order of operations. 
# Store 9, followed by 8, then 3.
# This suggests these stores may have the highest average customer spend or promotions, indicating stronger sales or perhaps more aggressive markdowns
# Note that 14/15 of the highest avg_cpi's have HIGH average temperatures (60F+) indicating that stores in regions with higher avg_temperatures have higher customer spend

SELECT Store, AVG(CPI) AS avg_cpi, COUNT(*) AS records, AVG(Temperature) as avg_temp
FROM feat_copy
GROUP BY Store
ORDER BY avg_cpi ASC
LIMIT 15;
# Lowest avg_cpi starts with 44, then 4, then 38
# Low average customer spend
# Lowest avg_cpi's temperatures vary much more. There are high and low average temperatures in the top 15 lowest avg_cpi stores


SELECT store, AVG(Temperature)
FROM feat_copy
GROUP BY store
ORDER BY 2 DESC
LIMIT 15;
# Store 33, 11, and 42 have highest average temperature


SELECT store, AVG(Temperature)
FROM feat_copy
GROUP BY store
ORDER BY 2 ASC
LIMIT 15;
# 7 26 and 16 lowest average temperatures

# Average Markdown by Holiday/Non-Holiday
SELECT 
  IsHoliday,
  AVG(MarkDown1) AS avg_md1,
  AVG(MarkDown2) AS avg_md2,
  AVG(MarkDown3) AS avg_md3,
  AVG(MarkDown4) AS avg_md4,
  AVG(MarkDown5) AS avg_md5
FROM Features
GROUP BY IsHoliday;
# Holiday markdown as expected higher for every markdown, except 5
# The kaggle link mentions that the markdowns precede holidays.
# So markdown 5 could be for a small holiday that is not discount heavy
# Markdown 3 and 1 highest during holiday
# Perhaps markdown 3 and 1 are big, discount-heavy holidays, like Christmas and Super Bowl


SELECT `Date`, AVG(MarkDown1) AS avg_md1, AVG(MarkDown2) AS avg_md2, AVG(MarkDown3) AS avg_md3, AVG(MarkDown4) AS avg_md4, AVG(MarkDown5) AS avg_md5
FROM feat_copy
GROUP BY `Date`
ORDER BY `Date`;
# No strong pattern of the average markdowns over time

# Let us copy the cleaned data to a new table just in case:

CREATE TABLE feat_cleaned
LIKE feat_copy;
INSERT feat_cleaned
SELECT * FROM feat_copy;

# There is more variables (unemployment, fuel_price) and EDA to analyse here but the rest will be done on other tools like Python using pandas for me to continue practicing my skills acorss different tools. We can also start modelling using python after that and make predictions.






















