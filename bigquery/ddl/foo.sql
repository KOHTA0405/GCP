create table dev_dwh.foo (
  id integer,
  name string(256),
  quantity integer,
  last_updated_at datetime default current_datetime('Asia/Tokyo')
)
partition by datetime_trunc(last_updated_at, day)
;