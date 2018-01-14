drop index if exists users_nickname;
drop index if exists forums_slug;
drop index if exists forums_user_id;
drop index if exists threads_slug;
drop index if exists threads_user_id;
drop index if exists threads_created;
drop index if exists threads_forum_slug;
drop index if exists posts_user_id;
drop index if exists posts_thread_id;
drop index if exists posts_parent_path;
drop index if exists posts_parent_path_gin;
drop index if exists posts_parent_path_2;
drop index if exists votes_thread_id;
drop index if exists forum_user_user_id;
drop index if exists forum_user_forum_id;

drop table if exists users cascade;
drop table if exists forums cascade;
drop table if exists votes cascade;
drop table if exists threads cascade;
drop table if exists posts cascade;
drop table if exists forum_user cascade;
drop trigger if exists set_parent_path on posts;
drop function if exists create_parent_path();

create table users (
    id serial primary key,
    nickname citext collate pg_catalog.ucs_basic unique not null,
    email citext unique,
    about text,
    fullname text
);

create index users_nickname on users(nickname);

create table forums (
    id serial primary key,
    user_id int references users(id) on delete cascade,
    title text not null,
    slug citext unique not null,
    posts_count int default 0,
    threads_count int default 0
);

create index forums_slug on forums(slug);
create index forums_user_id on forums(user_id);


create table threads (
    id serial primary key,
    forum_id int references forums(id) on delete cascade,
    user_id int references users(id) on delete cascade,
    title text not null,
    message text,
    created timestamptz default now(),
    slug citext unique,
    votes int default 0,
    user_nickname citext collate pg_catalog.ucs_basic not null,
    forum_slug citext not null
);

create index threads_slug on threads(slug);
create index threads_user_id on threads(user_id);
create index threads_created on threads(created);
create index threads_forum_slug on threads(forum_slug);


create table posts (
    id serial primary key,
    user_id int references users(id) on delete cascade,
    -- forum_id int references forums(id) on delete cascade,
    thread_id int references threads(id) on delete cascade,
    message text,
    created timestamptz default now(),
    is_edited boolean default false,
    parent_id int default 0,
    parent_path bigint [],
    forum_slug citext not null,
    user_nickname citext collate pg_catalog.ucs_basic not null
);

create index posts_user_id on posts(user_id, id);
create index posts_thread_id on posts(thread_id, id);
create index posts_parent_path on posts(parent_path);
create index posts_parent_path_gin on posts using gin (parent_path);
create index posts_parent_path_2 on posts ((parent_path[2]));


create table votes (
    id serial primary key,
    user_id int references users(id) on delete cascade,
    thread_id int references threads(id) on delete cascade,
    voice smallint default 0,
    unique (user_id, thread_id)
);

create index votes_thread_id on votes(thread_id);


create table forum_user (
    id serial primary key,
    user_id int references users(id) on delete cascade,
    forum_id int references forums(id) on delete cascade,
    unique (user_id, forum_id)
);


create index forum_user_user_id on forum_user(user_id);
create index forum_user_forum_id on forum_user(forum_id);

-- create function update_forum_user() returns trigger as
-- $$
--     begin
--         insert into forum_user (user_id, forum_id)
--                 values (%s, %s) on conflict do nothing;
--     end
-- $$ language plpgsql;

-- create trigger forum_user_update after insert on posts
--     for each row execute procedure update_forum_user();


create function create_parent_path() returns trigger as
$$
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
$$ language plpgsql;

create trigger set_parent_path after insert on posts
    for each row execute procedure create_parent_path();
