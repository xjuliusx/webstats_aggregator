WITH hits AS (
  SELECT * FROM goatcounter_hits
)
SELECT
  path,
  SUM(count) AS views
FROM hits
WHERE event = FALSE
GROUP BY 1
ORDER BY 2 DESC;
