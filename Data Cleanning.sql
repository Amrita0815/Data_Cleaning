-- Data Cleaning

-- CREATE TABLE world_layoffs.layoffs_staging 
-- LIKE world_layoffs.layoffs;

-- INSERT layoffs_staging 
-- SELECT * FROM world_layoffs.layoffs;

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
 
 select  	
 select  
FROM layoffs_staging2;




