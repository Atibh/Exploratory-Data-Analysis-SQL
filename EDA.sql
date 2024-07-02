-- DATA CLEANING
SELECT *
FROM layoffs;

-- step1. Removing duplicates
-- step2. Standardize the data 
-- step3. Dealing with Null values or blank values 
-- step4. Removing unnecassary column and rows 


CREATE TABLE layoffs_staging      -- COPYING ACTUAL DATA TO NEW TABLE (HERE WE COPYING COLUMN NAME)
LIKE layoffs;  



-- COPYING THE DATA TO NEW TABLE
INSERT layoffs_staging
SELECT *
FROM layoffs;  

SELECT *
FROM layoffs_staging;


SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM world_layoffs.layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1; 

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- step2. Standardize the data 

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;                -- here we can see problem with industry column as the there are two diffrent word to denote crypto currency

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT country -- here we see a problem wih country name 
FROM layoffs_staging2
ORDER BY 1; 

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) 
FROM layoffs_staging2
ORDER BY 1; 

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- changing date  type from text time series exploratory analysis
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y') 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2;

-- step3. Dealing with Null values or blank values

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2  -- distinct give unique value
ORDER BY industry;

-- checking for the value in industry which are blank or null
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;



-- let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Carvana';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Juul';
--  nothing wrong here( all 3 company, industry column) 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb';
-- it looks like airbnb is a travel, but the one column has blank space  in industry column


-- now creating joins of table layoffs_staging2 with itself 
SELECT *
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
     ON t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry ='')    -- here t1 is table where industry has null or blnk value and t2 is table where industry has not null value
AND t2.industry IS NOT NULL;

-- here we can see both column join
SELECT t1.industry,t2.industry
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
     ON t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry ='')    
AND t2.industry IS NOT NULL;

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1     -- updating industry column layoffs_staging2 t1(table 1 or actual table ) with table2  industry column according to comapny
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL   
AND t2.industry IS NOT NULL;


-- the company which has not laid of 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- deleting the data 

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- step4. Removing unnecassary column and rows 

-- Droping column from table  (row_num) 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;

-- Exploratory data analysis 

SELECT * 
FROM layoffs_staging2;

--  MAX total laid off in one go 

SELECT MAX(total_laid_off ), MAX(percentage_laid_off)  -- max percentage 1 shows all employed are laid off
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;


SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- sum of total laid off by company
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`),MAX(`date`)
FROM layoffs_staging2;

SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- sum of total laid off by COUNTRY(this it total in the past 3 years or in the dataset)
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year.

SELECT * 
FROM layoffs_staging2;

-- taking out the month from date column
-- grouping the layoff data per month

SELECT SUBSTRING(`date`,1,7) AS MONTH , SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC ;

-- rolling up (something like fibbonaci series)
-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY dates              -- here instead of month we are using dates just for not using keyword
ORDER BY dates ASC
)
SELECT dates,total_laid_off
,SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;


WITH Company_Year(company,years,total_laid_off) AS 
(
  SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
)
SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY RANKING ASC;

-- filter on looking top five company ranking per year
WITH Company_Year(company,years,total_laid_off) AS
(
  SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
)                                 -- using another cte
, Company_Year_Rank AS 
(
  SELECT *
  ,DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
  WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;
