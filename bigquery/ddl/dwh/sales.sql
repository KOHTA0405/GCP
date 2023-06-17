create table if not exists `dev_dwh.sales` (
    salesid int not null,
    listid int not null,
    sellerid int not null,
    buyerid int not null,
    eventid int not null,
    dateid int not null,
    qtysold int not null,
    pricepaid numeric,
    commission numeric,
    saletime datetime,
    last_updated_at datetime default current_datetime('Asia/Tokyo'),
    primary key(salesid) not enforced
)
partition by datetime_trunc(saletime, day)
;
