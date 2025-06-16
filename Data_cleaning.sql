# DATA CLEANING
#	1. Remove Duplicates
#	2. Standadize the Data
#	3. Null values or Blank values
#	4. Remove any Coloumns and rows that are not necessary

##########################	1. Remove Duplicates ###########################
SELECT *
FROM layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging 
LIKE lay_offs.layoffs;

INSERT layoffs_staging 
SELECT * 
FROM lay_offs.layoffs;

SELECT *
FROM layoffs_staging;

-- 1. Remove Duplicates

# First let's check for duplicates

SELECT * ,
ROW_NUMBER () OVER ( PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM layoffs_staging; # this is fine but we are making a CTE for it

WITH duplicate_cte AS 
( 
SELECT * ,
ROW_NUMBER () OVER ( PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;


-- let's just look at oda to confirm

SELECT *
FROM layoffs_staging
WHERE company = 'Oda' ;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- for real duplicates apply filter on each column so --

WITH duplicate_cte AS 
( 
SELECT * ,
ROW_NUMBER () OVER ( PARTITION BY company, location, industry, total_laid_off,`date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially
-- now you may want to write it like this:

WITH delete_cte AS #same cte as duplicate_cte
( 
SELECT * ,
ROW_NUMBER () OVER ( PARTITION BY company, location, industry, total_laid_off,`date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

DELETE
FROM delete_cte
WHERE row_num > 1 ;

 #Creating new row in table
		CREATE TABLE `layoffs_staging2` (
		`company` text,
		`location` text,
		`industry` text,
		`total_laid_off` int DEFAULT NULL,
		`percentage_laid_off` text,
		`date` text,
		`stage` text,
		`country` text,
		`funds_raised_millions` int DEFAULT NULL,
		`row_no` INT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

                                                                  

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER ( PARTITION BY company, location, industry, total_laid_off,`date`, 
stage, country, funds_raised_millions) AS row_no
FROM layoffs_staging ;

DELETE
FROM layoffs_staging2
WHERE row_no > 1 ;
 
SET SQL_SAFE_UPDATES = 0;

#WE MADE LAYOFFS_STAGING3 AS THERE WAS STILL DUPLICATE DATA 

		CREATE TABLE `layoffs_staging3` (
		`company` text,
		`location` text,
		`industry` text,
		`total_laid_off` int DEFAULT NULL,
		`percentage_laid_off` text,
		`date` text,
		`stage` text,
		`country` text,
		`funds_raised_millions` int DEFAULT NULL,
		`row_num` INT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

                                                                  

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER () OVER ( PARTITION BY company, location, industry, total_laid_off,`date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging ;

DELETE
FROM layoffs_staging3
WHERE row_num > 1 ;


##############################	2. Standadize the Data    #################################

SELECT company
FROM layoffs_staging3;

SELECT company, trim(company)
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET company = trim(company);

SELECT *
FROM layoffs_staging3;

SELECT industry -- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
FROM layoffs_staging3
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging3
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT industry
FROM layoffs_staging3;

SELECT DISTINCT location
FROM layoffs_staging3
ORDER BY 1 ;

SELECT DISTINCT country -- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
FROM layoffs_staging3
ORDER BY 1 ;

UPDATE layoffs_staging3
SET country = TRIM( TRAILING '.' FROM country) #removes specified character
WHERE country LIKE 'United States%' ;

-- Let's also fix the date columns:
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging3 ;

-- we can use str to date to update this field
UPDATE layoffs_staging3
SET `date` =STR_TO_DATE(`date`, '%m/%d/%Y') ;

-- now we can convert the data type properly
ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;

##############################-- 3. Look at Null Values ####################################3



SELECT *
FROM layoffs_staging3
WHERE industry IS NULL 
OR industry = '' ;

SELECT t1.industry, t2.industry 
FROM layoffs_staging3 AS t1
JOIN layoffs_staging3 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;

#first setting all blank values to null to avoid errors

UPDATE layoffs_staging3
SET industry = NULL
WHERE industry = '' ;

#Now creating our update statement to fill null industries

UPDATE layoffs_staging3 AS t1
JOIN layoffs_staging3 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL ;

#Checking
SELECT *
FROM layoffs_staging3
WHERE company = 'Airbnb' ;

#Now checking for total_laidoff and percentage_laidoff
SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ; 

#data is useless where there are no values for total_laidoff and percentage_laidoff so deleting it

DELETE 
FROM layoffs_staging3
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ; 


#####################	4. Remove any Coloumns and rows that are not necessary ###########
-- now deleting the extra column we add before i.e row_num

ALTER TABLE layoffs_staging3
DROP COLUMN row_num ;

SELECT *
FROM layoffs_staging3 ;

