---------------------------------------------------------------------------
-- Luxury Popup Sales:
-- Creating a staging table for popup sales for preliminary data QC
DROP TABLE IF EXISTS popups_stage;
CREATE TABLE popups_stage (
	event_id TEXT PRIMARY KEY,
	brand TEXT,
	region TEXT,
	city TEXT,
	location_type TEXT,
	event_type TEXT,
	start_date DATE,
	end_date DATE,
	lease_length_days INT,
	sku TEXT,
	product_name TEXT,
	price_usd NUMERIC(18,2),
	avg_daily_footfall INT,
	units_sold INT,
	sell_through_pct NUMERIC(18,2),
	start_date_iso DATE,
	end_date_iso DATE
);

	---------------------------------------------------------------------------
	-- Importing Python-cleaned CSV into popups_stage for preliminary data integrity checks/exploration
	SELECT * FROM popups_stage;
	
		-- Confirming NULL values
		SELECT * FROM popups_stage WHERE city IS NULL; -- 47 missing 'City' values
		SELECT * FROM popups_stage WHERE end_date_iso IS NULL; -- 36 missing 'End Date' values
		
		-- Replacing null city values with 'Unknown'
		UPDATE popups_stage 
		SET city = 'Unknown'
		WHERE city IS NULL;
		SELECT DISTINCT(city) FROM popups_stage;
		
		-- Pre-emptively replacing 'New York' with 'New York City' to match City Population information to be imported
		UPDATE popups_stage
		SET city = 'New York City'
		WHERE city = 'New York';
		SELECT DISTINCT(city) FROM popups_stage WHERE city LIKE '%New York%';

---------------------------------------------------------------------------
-- City Population information:		
-- Creating a staging table for city demographic information for preliminary data QC
DROP TABLE IF EXISTS city_stage;
CREATE TABLE city_stage (
	geoname_id TEXT PRIMARY KEY,
	name TEXT,
	country_code TEXT,
	population INT
); -- Import "geonames_cities_population.csv" at this stage

	-- Checking for duplicate city names (e.g. "Berlin, Germany" vs "Berlin, USA" etc)
	SELECT name, country_code, population
	FROM city_stage
	WHERE LOWER(name) = 'berlin'
	ORDER BY population DESC; 
	-- 8 different cities called "Berlin" 
	-- Most relevant one would be the one with the highest population
	
	SELECT * FROM city_stage WHERE name LIKE '%New York%';
	SELECT name, COUNT(name) AS duplicate_count FROM city_stage GROUP BY name HAVING COUNT(name) > 1 ORDER BY COUNT(name) DESC;
	
-- Joining "popups_stage" and "city_stage" tables into combined analysis table
CREATE EXTENSION IF NOT EXISTS unaccent; -- Ensures accent-insensitive city matching
DROP TABLE IF EXISTS popups_analysis;

CREATE TABLE popups_analysis AS
WITH city_best AS (
  SELECT DISTINCT ON (unaccent(lower(name)))
         name,
         population
  FROM city_stage
  ORDER BY unaccent(lower(name)), population DESC
)
SELECT
    ps.event_id,
    ps.brand,
    ps.region,
    ps.city,                      -- already normalised upstream
    cb.name        AS city_name,
    cb.population  AS city_population,
    ps.location_type,
    ps.event_type,
    ps.start_date_iso AS start_date,
    COALESCE(ps.end_date_iso,
             ps.start_date_iso + ps.lease_length_days * INTERVAL '1 day')::date AS end_date,
    ps.lease_length_days,
    ps.sku,
    ps.product_name,
    ps.price_usd,
    ps.avg_daily_footfall,
    ps.units_sold,
    ps.sell_through_pct
FROM popups_stage ps
LEFT JOIN city_best cb
  ON unaccent(lower(cb.name)) = unaccent(lower(ps.city))
;

	-- Performing QC checks on popups_analysis
	SELECT * FROM popups_analysis WHERE city_population IS NULL; -- Check if the New York rows have had their population counts attached successfully	
	SELECT * FROM popups_analysis WHERE end_date IS NULL; -- 0 missing 'end_date' values

	-- Remove now-irrelevant city_name column
	ALTER TABLE popups_analysis
		DROP COLUMN city_name;
	
---------------------------------------------------------------------------
-- Creating business-relevant metric columns:		

	-- Adding new business-relevant metrics to table
	ALTER TABLE popups_analysis
		ADD COLUMN total_popup_days INT, -- Used to cross-check with length_lease_days
		ADD COLUMN total_revenue_usd NUMERIC(18,2), -- units_sold * price_usd per unit for each SKU
		ADD COLUMN revenue_per_day NUMERIC(18,2), -- total_revenue_usd / total_popup_days
		ADD COLUMN total_footfall INT, -- avg_daily_footfall * total_popup_days
		ADD COLUMN conversion_per_1000_visitors NUMERIC(18,4) -- (units_sold / total_footfall) * 1000 visitors
		;

	
	-- Populating new business-relevant metrics with calculations
	UPDATE popups_analysis
		SET total_popup_days = (end_date - start_date);
		SELECT * FROM popups_analysis WHERE lease_length_days <> total_popup_days;
		
		UPDATE popups_analysis
			SET total_revenue_usd = (units_sold * price_usd);
		
		UPDATE popups_analysis
			SET revenue_per_day = total_revenue_usd / NULLIF(total_popup_days, 0);
			
		UPDATE popups_analysis
			SET total_footfall = (avg_daily_footfall * total_popup_days);
		
		UPDATE popups_analysis
			SET conversion_per_1000_visitors = (units_sold::float / NULLIF(total_footfall, 0)) * 1000; -- Multipled by 1000 for human readability
		
	-- Data integrity: Checking for impossible/implausible values
		SELECT
		  SUM(CASE WHEN price_usd <= 0 THEN 1 ELSE 0 END) AS bad_price,
		  SUM(CASE WHEN total_popup_days <= 0 THEN 1 ELSE 0 END) AS bad_days,
		  SUM(CASE WHEN units_sold < 0 THEN 1 ELSE 0 END) AS bad_units,
		  SUM(CASE WHEN avg_daily_footfall < 0 THEN 1 ELSE 0 END) AS bad_footfall,
		  SUM(CASE WHEN sell_through_pct < 0 OR sell_through_pct > 100 THEN 1 ELSE 0 END) AS bad_stp
		FROM popups_analysis;
		-- 0 "bad" rows

---------------------------------------------------------------------------
-- Basic Data Exploration

	-- Popups per brand in APAC
	SELECT brand, COUNT(*) AS rows_apac
	FROM popups_analysis
	WHERE region = 'Asia-Pacific'
	GROUP BY brand
	ORDER BY rows_apac DESC;
	-- #1) Shiseido - 28 popups
	-- #2) Cle de Peau Beaute - 27 popups
	-- #3) Charlotte Tilbury -- 24 popups
		
	-- Counting which city has the most popups per region
	SELECT region, city, COUNT(*) AS Popup_Count
	FROM popups_analysis
	GROUP BY region, city
	ORDER BY region, Popup_Count DESC;
	-- #1) Hong Kong - 108 popups
	-- #2) Tokyo - 89 popups
	-- #3) Singapore - 85 popups
	
	-- Top-performing brands in APAC
	SELECT brand, SUM(total_revenue_usd) AS brand_revenue
	FROM popups_analysis
	WHERE region = 'Asia-Pacific' AND city <> 'Unknown'
	GROUP BY brand
	ORDER BY brand_revenue DESC;
	-- #1) Cle de Peau Beaute - $58,923,134
	-- #2) Hourglass - $4,766,050
	-- #3) La Prairie - $4,076,906
	
	-- Date coverage of dataset
	SELECT MIN(start_date) AS min_start, MAX(end_date) AS max_end
	FROM popups_analysis; 
	-- earliest start: 2024-02-18, latest end: 2025-08-11
		
	-- Date coverage of dataset (APAC only)
	SELECT MIN(start_date) AS min_start, MAX(end_date) AS max_end
	FROM popups_analysis WHERE region = 'Asia-Pacific'; 
	-- earliest start: 2024-02-19, latest end: 2025-08-09

