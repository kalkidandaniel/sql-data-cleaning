-- Data Cleaning


SELECT * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT *
FROM layoffs_staging;


INSERT layoffs_stagilayoffs_staginglayoffs_staginglayoffslayoffs_staginglayoffs_stagingng
SELECT *
FROM layoffs;



-- Remove Duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, industry, total_laid_off, percentage_laid_off, `date`)AS row_num
FROM layoffs_staging;



WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'casper';


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)AS row_num
FROM layoffs_staging
)
delete
FROM duplicate_cte
WHERE row_num > 1;


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
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised_millions)AS row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


--  Standardize the data (finding issue and fix it)


SELECT company ,TRIM(company)
FROM layoffs_staging2 ;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT distinct country
FROM layoffs_staging2 
order by 1;

SELECT *
FROM layoffs_staging2
WHERE industry like 'crypto%';

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry Like 'crypto%';

select distinct COUNTRY, TRIM(TRAILING '.' FROM COUNTRY)
FROM layoffs_staging2
WHERE country LIKE 'united states%'
ORDER BY 1;

UPDATE layoffs_staging2
SET country = trim(TRAiLING '.' FROM COUNTRY)
WHERE country LIKE 'united states%';

SELECT `DATE`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` =str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY column `date` DATE;

SELECT*
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

select *
from layoffs_staging2
where company = 'airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company
	 and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company
     set t1.industry = t2.industry
     where t1.industry is null 
and t2.industry is not null;

     select *
     from layoffs_staging2;
     

SELECT*
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;
     
delete
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;
     
     alter table layoffs_staging2
     drop column row_num;
     





