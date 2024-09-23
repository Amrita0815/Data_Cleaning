-- Data Cleaning

-- 1. Remove Duplicates
-- 2. Standerdize the Data
-- 3. Null Values or blank values
-- 4. Remove any columns and rows that are not necessary - few ways


SELECT *
FROM layoffs;


CREATE TABLE  layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
 SELECT *
 FROm layoffs;
  
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
 FROM layoffs_staging;
 
 
  -- Remove Duplicates
 WITH duplicate_cte AS 
 (
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage
 , country, funds_raised_millions) as row_num
 FROM layoffs_staging
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 SELECT *
 FROM layoffs_staging2;
 
 INSERT INTO layoffs_staging2
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage
 , country, funds_raised_millions) as row_num
 FROM layoffs_staging;
 
 DELETE
 FROM layoffs_staging2
 WHERE row_num > 1;
 
 SELECT *
 FROM layoffs_staging2;
 
 
 
 -- Standardizing data
 
  -- Removing the extra spaces 
select  company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company =  TRIM(company);
 
 SELECT DISTINCT industry
 FROM layoffs_staging2
 GROUP BY 1;
 
 SELECT *
 FROM layoffs_staging2
 WHERE industry LIKE 'crypto%';

 UPDATE layoffs_staging2
 SET industry = 'crypto'
 WHERE industry LIKE 'crypto%';
 
 SELECt DISTINCT industry
 FROM layoffs_staging2;
 
 select  distinct location
FROM layoffs_staging;

SELECT  DISTINCT  COUNTRY
FROM layoffs_staging2
ORDER BY 1;

-- United States had a 2nd categories (United States and United States.) 
-- removed the period at the end of the 2nd one. 
SELECT DISTINCT country, TRIM(TRAILING '.'  FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.'  FROM country)
WHERE country LIKE  'United States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

SELECT *
FROm layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT distinct industry
FROm layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
 
- Removing the NUll Values
SELECT *
FROm layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECt *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company
Where (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
ON t1.company = t2.company
SET t1.industry = t2.industry
Where t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
Where industry = '';

SELECT *
FROM layoffs_staging2;


SELECT *
FROm layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Nulls in total_laid_off and percentage_laid_off
DELETE 
FROm layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- Removing row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;



