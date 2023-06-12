create table if not exists `dev_dwh.category` (
    catid int not null,
    catgroup string(10),
    catname string(10),
    catdesc string(50)
);
