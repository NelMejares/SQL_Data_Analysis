-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicate
-- 2. Standardize Data 
-- 3. Null Values or Blank Values
-- 4. Remove Columns

-- Creating backup table

CREATE TABLE layoffs_stagging
LIKE layoffs;

INSERT layoffs_stagging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_stagging;

-- End Backup

-- Removing Duplicate

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

WITH dup_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging
)
SELECT *
FROM dup_cte
WHERE row_num > 1;

-- creating new table for row_num for deleting duplicate
CREATE TABLE `layoffs_stagging2` (
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
FROM layoffs_stagging2;

INSERT INTO layoffs_stagging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

SELECT *
FROM layoffs_stagging2
WHERE row_num > 1;

DELETE
FROM layoffs_stagging2
WHERE row_num > 1;

SELECT *
FROM layoffs_stagging2;

-- Duplicate Removed

-- Standardizing  Data

-- removing whitespace
SELECT company, (TRIM(company))
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET company = TRIM(company);

-- setting Crypto as default value
SELECT DISTINCT industry
FROM layoffs_stagging2
ORDER BY 1;

SELECT *
FROM layoffs_stagging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- removing extra characters
SELECT DISTINCT country
FROM layoffs_stagging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_stagging2
ORDER BY 1;

UPDATE layoffs_stagging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- changing date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM  layoffs_stagging2;

UPDATE layoffs_stagging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- change format from text to date
ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE;

-- checking for null values
SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL
OR industry = '';

-- adding industry from same company
SELECT *
FROM layoffs_stagging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- removing data where values is null
DELETE
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_stagging2;

ALTER TABLE layoffs_stagging2
DROP COLUMN row_num;