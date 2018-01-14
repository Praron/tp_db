create trigger set_parent_path after insert on posts
    for each row execute procedure create_parent_path();
