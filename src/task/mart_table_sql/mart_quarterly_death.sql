-- 建立各季度各類型車禍致死人數的分析表
-- 1. 將車禍大類別分得更乾淨
CREATE OR REPLACE VIEW v1_dim_accident_type AS
	(SELECT
		accident_type_id, 
        accident_type_major, 
		CASE
			WHEN accident_type_major = '人與汽(機)車' THEN '人與車'
			WHEN accident_type_major = '人與汽機車' THEN '人與車'
			WHEN accident_type_major = '汽(機)車本身' THEN '車輛本身'
			ELSE accident_type_major
		END AS accident_type_major_grouped
			FROM dim_accident_type);
            
-- 2. 由於需要車禍日期、死傷人數，所以取用main與day表
CREATE OR REPLACE VIEW  v2_typegroup_main_day AS
	(SELECT v1.accident_type_major_grouped,
			YEAR(d.accident_date) AS accident_year,
			QUARTER(d.accident_date) AS accident_quarter,
			m.accident_time,
			m.death_count, 
            m.injury_count
		FROM fact_accident_main m -- 大表JOIN小表
			JOIN v1_dim_accident_type v1
			ON m.accident_type_id = v1.accident_type_id
					JOIN dim_accident_day d
					ON m.day_id = d.day_id);

-- 3. 建立Mart層圖表
CREATE TABLE IF NOT EXISTS mart_quarterly_death AS
	(SELECT
		accident_year, accident_quarter,
		accident_type_major_grouped,
		SUM(death_count) AS `death_quarterly_counts`
			FROM v2_typegroup_main_day v2
				GROUP BY accident_year, accident_quarter, accident_type_major_grouped
					ORDER BY accident_year, accident_quarter);

DROP VIEW v1_dim_accident_type, v2_typegroup_main_day;