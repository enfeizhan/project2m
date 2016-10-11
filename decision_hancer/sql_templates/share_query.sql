SELECT
  code
  ,"date"
  ,open_price
  ,high_price
  ,low_price
  ,close_price
  ,adj_close_price
  ,volume
from share_price
where 
  code in ({codes})
  and "date" between '{start_date}' and '{end_date}'
;
