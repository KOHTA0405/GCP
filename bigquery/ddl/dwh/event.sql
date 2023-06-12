create table if not exists `dev_dwh.event` (
    eventid int not null,
    venueid int not null,
    catid int not null,
    dateid int not null,
    eventname string(200),
    starttime datetime
);
