SELECT
  code
  ,"date"
  ,open_price
  ,high_price
  ,low_price
  ,close_price
  ,adj_close_price
  ,volume
FROM share_price
WHERE
  code IN ({codes})
  AND "date" BETWEEN '{start_date}' AND '{end_date}'
;
