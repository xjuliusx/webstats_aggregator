WITH hits AS (
  SELECT
    *,
    CASE
      WHEN path LIKE 'click-%' THEN SUBSTR(path, 7)
      ELSE path
    END AS event_label
  FROM goatcounter_hits
)
SELECT
  event_label,
  COUNT(*)
FROM hits
WHERE event = TRUE
GROUP BY 1
ORDER BY 2 DESC;
