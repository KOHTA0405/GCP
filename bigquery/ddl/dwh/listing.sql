create table if not exists `dev_dwh.listing` (
    listid int not null,
    sellerid int not null,
    eventid int not null,
    dateid int not null,
    numtickets int not null,
    priceperticket numeric,
    totalprice numeric,
    listtime datetime
);
