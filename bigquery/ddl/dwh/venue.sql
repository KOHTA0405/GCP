create table if not exists `dev_dwh.venue` (
    venueid int not null,
    venuename string(100),
    venuecity string(30),
    venuestate string(2),
    venueseats int,
    last_updated_at datetime default current_datetime('Asia/Tokyo'),
    primary key(venueid) not enforced
);
