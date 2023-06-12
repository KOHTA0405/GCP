create table if not exists `dev_dwh.date` (
    dateid int not null,
    caldate date not null,
    day string(3) not null,
    week int not null,
    month string(5) not null,
    qtr string(5) not null,
    year int not null,
    holiday bool default null,
    last_updated_at datetime default current_datetime('Asia/Tokyo')
);
