-- 針對與行人直接相關的車禍案件，分析主要肇因比例分析表
-- 1. 將車禍大類別 清得更乾淨。
CREATE OR REPLACE VIEW v1_dim_accident_type AS
	(SELECT 
		d.*,
		CASE
			WHEN accident_type_major = "人與汽(機)車" THEN "人與車"
			WHEN accident_type_major = "人與汽機車" THEN "人與車"
			WHEN accident_type_major = "汽(機)車本身" THEN "車輛本身"
			ELSE accident_type_major
		END AS accident_type_major_grouped
			FROM dim_accident_type d);

-- 2. 由於需要死傷人數、日期，所以再取用main與day，且JOIN後只留下"人與車"這類與行人直接相關的車禍案件，順便把資料列減少
CREATE OR REPLACE VIEW v2_main_v1type AS
	(SELECT m.*,
			YEAR(d.accident_date) AS accident_year,
			QUARTER(d.accident_date) AS accident_quarter,
			v1.accident_type_major_grouped
		FROM fact_accident_main m
			JOIN v1_dim_accident_type v1
			ON m.accident_type_id = v1.accident_type_id
					JOIN dim_accident_day d
					ON m.day_id = d.day_id
						WHERE accident_type_major_grouped = "人與車");

			
-- 3. 將用路人身份大類別 清得更乾淨。然後與v2串接
CREATE OR REPLACE VIEW v3_main_v1type_human AS
	(SELECT v2.accident_year,
			v2.accident_quarter,
            v2.death_count,
            v2.injury_count,
            h.cause_analysis_minor_individual,
			CASE
				WHEN cause_analysis_major_individual = "行人(或乘客)" THEN "非駕駛者(行人或乘客)"
				WHEN cause_analysis_major_individual = "非駕駛者" THEN "非駕駛者(行人或乘客)"
				WHEN cause_analysis_major_individual = "駕駛人" THEN "駕駛者"
				WHEN cause_analysis_major_individual = "無(車輛駕駛者因素)" THEN "駕駛者"
				ELSE cause_analysis_major_individual          
			END AS cause_analysis_major_individual_grouped
		FROM fact_accident_human h
			JOIN v2_main_v1type v2
				ON h.accident_id = v2.accident_id);
        
-- 4. 建立Mart層圖表
CREATE TABLE IF NOT EXISTS mart_quarterly_pedestrian_related_causes_top5 AS
	-- 使用Common Table Expression語法生成臨時資料表，並宣告為ranked_behaviors資料表
	WITH ranked_behaviors AS 
			(SELECT
						accident_year,
						accident_quarter,
						cause_analysis_major_individual_grouped AS `type of road user`,
						cause_analysis_minor_individual AS `behavior`,
						COUNT(cause_analysis_minor_individual) as `counts of behavior`,
						RANK() OVER (
									  PARTITION BY accident_year, accident_quarter
									  ORDER BY COUNT(cause_analysis_minor_individual) DESC
									) as `rank`
				FROM v3_main_v1type_human
					GROUP BY accident_year, accident_quarter, `type of road user`, `behavior`
			  )
		-- 主查詢區:
		SELECT  accident_year, accident_quarter, 
				`type of road user`, `behavior`, `counts of behavior`, `rank`
			FROM ranked_behaviors
				WHERE `rank` <= 5
					ORDER BY  accident_year, accident_quarter, `rank`;

DROP VIEW v1_dim_accident_type, v2_main_v1type, v3_main_v1type_human;