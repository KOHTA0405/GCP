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
    saletime datetime
);
