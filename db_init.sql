drop table if exists users cascade;
drop table if exists forums cascade;
drop table if exists votes cascade;
drop table if exists threads cascade;
drop table if exists posts cascade;
drop trigger if exists set_parent_path on posts;
drop function create_parent_path();

create table users (
    id serial primary key,
    nickname citext collate pg_catalog.ucs_basic unique not null,
    email citext unique,
    about text,
    fullname text
);

create table forums (
    id serial primary key,
    user_id int references users(id) on delete cascade,
    title text not null,
    slug citext unique not null,
    posts_count int default 0,
    threads_count int default 0
);

create table threads (
    id serial primary key,
    forum_id int references forums(id) on delete cascade,
    user_id int references users(id) on delete cascade,
    title text not null,
    message text,
    created timestamptz default now(),
    slug citext unique,
    votes int default 0
);

create table posts (
    id serial primary key,
    user_id int references users(id) on delete cascade,
    -- forum_id int references forums(id) on delete cascade,
    thread_id int references threads(id) on delete cascade,
    message text,
    created timestamptz default now(),
    is_edited boolean default false,
    parent_id int default 0,
    parent_path bigint []
);

create table votes (
    id serial primary key,
    user_id int references users(id) on delete cascade,
    thread_id int references threads(id) on delete cascade,
    voice smallint default 0,
    constraint votes_uniq_pair unique (user_id, thread_id)
);


create function create_parent_path() returns trigger as
$create_parent_path$
    declare
        old_path int[];
        new_path int[];
    begin
        if new.parent_id = 0 then
            update posts set parent_path = array [0, new.id] where id = new.id;
        else
            select parent_path into old_path from posts where id = new.parent_id;
            new_path := old_path || new.id;
            update posts set parent_path = new_path where id = new.id;
        end if;
        return new;
    end;
$create_parent_path$ language plpgsql;

create trigger set_parent_path after insert on posts
    for each row execute procedure create_parent_path();
