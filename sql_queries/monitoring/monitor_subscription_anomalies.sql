select * from spark_dwh.subscriptions_raw 
where end_date < current_date() and status = 'Active'
