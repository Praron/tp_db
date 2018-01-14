from sys import stderr
from os import getpid
import pytz
from datetime import datetime, timedelta
from contextlib import contextmanager
import numbers
from flask import Blueprint, request, Response, json, abort
from connect_db import connect_DB
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

forum_blueprint = Blueprint('forum', __name__)

CONFLICT = 409
CREATED = 201
OK = 200

# class ProcessSafePoolManager:
#     def __init__(self, *args, **kwargs):
#         self.last_seen_process_id = getpid()
#         self.args = args
#         self.kwargs = kwargs
#         self._init()


#     def _init(self):
#         self._pool = ThreadedConnectionPool(*self.args, **self.kwargs)


#     def getconn(self):
#         current_pid = getpid()
#         if not current_pid == self.last_seen_process_id:
#             self._init()
#             self.last_seen_process_id = current_pid
#         return self._pool.getconn()


#     def putconn(self, conn):
#         return self._pool.putconn(conn)

# pool = ProcessSafePoolManager(1, 10, host='localhost', database='postgres', user='postgres', password='ident')

conn = connect_DB()  # get from pool
@contextmanager
def get_DB_cursor():
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # yield conn
        yield cur
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        cur.close()
        # conn.close()  # return to pool

# @contextmanager
# def get_DB_cursor():
#     try:
#         conn = pool.getconn()
#         with conn:
#             with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#                yield cur
#     except:
#         # conn.rollback()
#         raise
#     finally:
#         pool.putconn(conn)


def make_response(status, to_json):
    return Response(
        response=json.dumps(to_json),
        status=status,
        mimetype="application/json"
    )


def get_user_or_404(user_id=None, email=None, nickname=None, abort404=True):
    with get_DB_cursor() as cur:
        if user_id is not None:
            argument = user_id
            sql = 'select * from users where id = %s;'
            cur.execute(sql, (user_id,))
            user = cur.fetchone()
        elif nickname is not None:
            argument = nickname
            sql = f'''select * from users where nickname = %(nickname)s {'or email = %(email)s' if email else ''}'''
            cur.execute(sql, {'nickname': nickname, 'email': email})
            user = cur.fetchone()
        elif email is not None:
            argument = email
            sql = f'''select * from users where email = %s'''
            cur.execute(sql, (email,))
            user = cur.fetchone()
        else: 
            raise Exception('No argument!')
        return (user or abort(404, argument)) if abort404 else user


def get_forum_or_404(slug_or_id=None, abort404=True):
    is_number = lambda x: x.isdigit()

    if slug_or_id is not None:
        with get_DB_cursor() as cur:
            sql = f'''select * from forums where {'id' if is_number(str(slug_or_id)) else 'slug'} = %s;'''
            cur.execute(sql, (slug_or_id,))
            forum = cur.fetchone()
            return (forum or abort(404, slug_or_id)) if abort404 else forum
    raise Exception('No argument!')


def get_post_or_404(post_id=None):
    if post_id is not None:
        with get_DB_cursor() as cur:
            sql = 'select * from posts where id = %s;'
            cur.execute(sql, (post_id,))
            post = cur.fetchone()
            return post or abort(404, post_id)
    raise Exception('No argument!')


def get_thread_or_404(forum_id=None, slug_or_id=None, abort404=True):
    if slug_or_id is not None:
        is_number = lambda x: x.isdigit()

        with get_DB_cursor() as cur:
            sql = f'''select * from threads where
                {'id' if is_number(slug_or_id) else 'slug'} = %(slug_or_id)s'''
            cur.execute(sql, {'forum_id': forum_id, 'slug_or_id': slug_or_id})
            thread = cur.fetchone()
            if thread is not None:
                thread = replace_time_format(thread)
            return (thread or abort(404, slug_or_id)) if abort404 else thread
    raise Exception('No argument!')


def get_parent_or_409(parent_id=None, thread_id=None):
    if parent_id is not None:
        with get_DB_cursor() as cur:
            sql = 'select * from posts where id = %(parent_id)s and thread_id = %(thread_id)s'
            cur.execute(sql, {'parent_id': parent_id, 'thread_id': thread_id})
            post = cur.fetchone()
            return post or abort(409, parent_id)
    raise Exception('No argument!')


def create_forum(user_id, title, slug):
    with get_DB_cursor() as cur:
        sql = 'insert into forums (user_id, title, slug) values (%s, %s, %s) returning *'
        cur.execute(sql, (user_id, title, slug))
        return cur.fetchone()


def replace_time_format(row):
    time = row['created']
    utc_time = time.astimezone(pytz.utc)
    row['created'] = utc_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return row


def create_thread(user, forum, title, message, slug=None, created=None):
    with get_DB_cursor() as cur:
        created = created or str(datetime.now())
        sql = f'''update forums set threads_count = threads_count + 1 where id = %(forum_id)s;
                  insert into forum_user (user_id, forum_id)
                    values (%(user_id)s, %(forum_id)s) on conflict do nothing;
                  insert into threads (user_id, forum_id, title, message, created, user_nickname, forum_slug
                  {', slug' if slug else ''}) 
                  values (%(user_id)s, %(forum_id)s, %(title)s, %(message)s, %(created)s, %(user_nickname)s, %(forum_slug)s {', %(slug)s' if slug else ''})
                  returning *'''

        args = {'user_id': user['id'], 'forum_id': forum['id'], 'title': title, 'slug': slug, 'message': message, 'created': created, 'user_nickname': user['nickname'], 'forum_slug': forum['slug']}
        cur.execute(sql, args)
        return replace_time_format(cur.fetchone())


def create_user(nickname, fullname, email, about):
    with get_DB_cursor() as cur:
        sql = 'insert into users (nickname, fullname, email, about) values (%s, %s, %s, %s) returning *'
        cur.execute(sql, (nickname, fullname, email, about))
        return cur.fetchone()


def get_dict_part(dictionary, keys):
    return {key: dictionary[key] for key in keys}


def get_user_info_or_404(nickname=None, email=None, with_nickname=False, in_list=False):
    with get_DB_cursor() as cur:
        sql = f'''select {'nickname, ' if with_nickname else ''} email, fullname, about
                  from users where {'nickname = %(nickname)s' if nickname else ''}
                                   {' or ' if nickname and email else ''}
                                   {'email = %(email)s' if email else ''}'''
        cur.execute(sql, {'nickname': nickname, 'email': email})
        return (cur.fetchall() if in_list else cur.fetchone()) or abort(404, nickname)
    


def get_forum_info(forum, user):
    info = get_dict_part(forum, ['slug', 'posts_count', 'title', 'threads_count'])
    info['user'] = user['nickname']
    info['forum'] = info['slug']
    info['threads'] = info['threads_count']
    info['posts'] = info['posts_count']
    return info


def get_thread_info(thread, forum=None, user=None, with_slug=True):
    forum = forum or get_forum_or_404(slug_or_id=thread['forum_id'])
    user = user or get_user_or_404(user_id=thread['user_id'])
    info = get_dict_part(thread, ['id', 'title', 'slug', 'message', 'created', 'votes'])
    info['author'] = user['nickname']
    info['forum'] = forum['slug']
    if not with_slug:
        del info['slug']
    return info


def get_threads_info(forum_name, limit=None, since=None, is_desc=False):
    is_desc = True if is_desc == 'true' else False
    with get_DB_cursor() as cur:
        if since is not None:
            since = since.replace('T', ' ').replace('Z', '')
            if since[-6] == '+' or since[-6] == '-':
                since = since[:-3] + since[-2:]
                # since = datetime.strptime(since, '%Y-%m-%d %H:%M:%S.%f%z') + timedelta(hours=3)
            else:
                since += '000'
                since = datetime.strptime(since, '%Y-%m-%d %H:%M:%S.%f') + timedelta(hours=3)
                since = format(since, '%Y-%m-%d %H:%M:%S.%f')

        sql = f'''
            select
                t.user_nickname as author,
                t.created,
                t.forum_slug as forum,
                t.id,
                t.message,
                t.slug,
                t.title,
                t.votes
            from threads as t
            where t.forum_slug = %(forum_slug)s
            {f"and t.created {'<=' if is_desc else '>='} %(since)s" if since else ''}
            order by t.created {'desc' if is_desc else ''}
            {f'limit %(limit)s' if limit else ''}
            '''
        cur.execute(sql, {'forum_slug': forum_name, 'since': since, 'limit': limit})
        threads = cur.fetchall()
        return list(map(replace_time_format, threads))


def get_forum_users_info(forum_id, limit=None, since=None, is_desc=False):
    with get_DB_cursor() as cur:
        sql = f'''
            select
                u.nickname,
                u.fullname,
                u.email,
                u.about
            from users as u 
                inner join forum_user as fu on u.id = fu.user_id
            where fu.forum_id = %(forum_id)s
            {f"and u.nickname {'<' if is_desc else '>'} %(since)s" if since else ''}
            order by u.nickname {'desc' if is_desc else ''}
            {f'limit %(limit)s' if limit else ''}
            '''
        cur.execute(sql, {'forum_id': forum_id, 'since': since, 'limit': limit})
        return cur.fetchall()


def change_post_message(post_id, message=None):
    with get_DB_cursor() as cur:
        if message:
            sql = 'select * from posts where id = %s'
            cur.execute(sql, (post_id,))
            old_post = cur.fetchone() or {} 
            if old_post.get('message') != message:
                sql = '''update posts set message = %s, is_edited = true where id = %s returning *'''
                cur.execute(sql, (message, post_id))
        post = get_post_info(post_id, thread_as_id=True)['post']
        return post


def get_post_info(post_id, what_to_show=None, thread_as_id=False):
    what_to_show = what_to_show or []
    is_author_need = 'user' in what_to_show
    is_thread_need = 'thread' in what_to_show
    is_forum_need = 'forum' in what_to_show
    answer = {}
    with get_DB_cursor() as cur:
        sql = f'''
            select p.user_nickname as author,
            p.created,
            p.forum_slug as forum,
            p.id,
            p.is_edited as "isEdited",
            p.message,
            p.parent_id as parent,
            {'t.id' if thread_as_id else 't.slug'} as thread,
            t.id as thread_id
            from posts as p
                join threads as t on p.thread_id = t.id
            where p.id = %s
            '''
        cur.execute(sql, (post_id,))
        post = cur.fetchone()

        answer['post'] = replace_time_format(post)

        forum = None
        if is_author_need:
            user = get_user_or_404(nickname=post['author'])
            answer['author'] = user
        if is_thread_need:
            forum = forum or get_forum_or_404(slug_or_id=post['forum'])
            thread = get_thread_or_404(forum_id=forum['id'], slug_or_id=(str(post['thread'] or post['thread_id'])))
            answer['thread'] = get_thread_info(thread, forum)
        if is_forum_need:
            forum = forum or get_forum_or_404(slug_or_id=post['forum'])
            user = get_user_or_404(user_id=forum['user_id'])
            answer['forum'] = get_forum_info(forum, user)

        post['thread'] = post['thread'] or post['thread_id']
        del post['thread_id']

        return answer


def drop_forum():
    with open('db_init.sql', 'r') as sql:
        with get_DB_cursor() as cur:
            sql = cur.mogrify(sql.read() + '\n').decode('utf-8')
            cur.execute(sql)


def get_forum_status():
    with get_DB_cursor() as cur:
        answer = {}
        sql = 'select count(*) as c from forums'
        cur.execute(sql)
        answer['forum'] = cur.fetchone()['c']
        sql = 'select count(*) as c from posts'
        cur.execute(sql)
        answer['post'] = cur.fetchone()['c']
        sql = 'select count(*) as c from threads'
        cur.execute(sql)
        answer['thread'] = cur.fetchone()['c']
        sql = 'select count(*) as c from users'
        cur.execute(sql)
        answer['user'] = cur.fetchone()['c']
        return answer


def add_posts(thread_id, posts, forum_id, forum_slug):
    if not posts:
        return []
    time = str(datetime.now())
    with get_DB_cursor() as cur:
        posts_len = len(posts) - 1
        sql = f'''update forums set posts_count = posts_count + {len(posts)} where id = %s;
             insert into forum_user (user_id, forum_id)
                values (%s, %s) {', (%s, %s)' * posts_len} on conflict do nothing;
             insert into posts (user_id, thread_id, message, created, parent_id, forum_slug, user_nickname) values
             (%s, %s, %s, %s, %s, %s, %s) {', (%s, %s, %s, %s, %s, %s, %s)' * posts_len} returning *'''
        args = [forum_id]
        for post in posts:
            user = get_user_or_404(nickname=post['author'])
            args += (user['id'], forum_id)
        for post in posts:
            user = get_user_or_404(nickname=post['author'])
            if post.get('parent'):
                get_parent_or_409(parent_id=post['parent'], thread_id=thread_id)                    
            args += (user['id'], thread_id, post['message'], time, post.get('parent', 0), forum_slug, post['author'])
        cur.execute(sql, args)
        return cur.fetchall()


def change_user(nickname, about=None, email=None, fullname=None):
    with get_DB_cursor() as cur:
        if about is None and email is None and fullname is None:
            sql = 'select * from users where nickname = %(nickname)s'
        else:
            sql = f'''update users set {'about = %(about)s' if about else ''} {',' if about and (email or fullname) else ''}
                                       {'email = %(email)s' if email else ''} {',' if email and fullname else ''}
                                       {'fullname = %(fullname)s' if fullname else ''}
                                       where nickname = %(nickname)s returning *'''
        cur.execute(sql, {'nickname': nickname, 'about': about, 'email': email, 'fullname': fullname})
        return cur.fetchone()


def change_thread(thread, message=None, title=None):
    with get_DB_cursor() as cur:
        if message is None and title is None:
            sql = 'select * from threads where id = %(thread_id)s' 
        else:
            sql = f'''update threads set {'message = %(message)s' if message else ''}
                                         {',' if message and title else ''}
                                         {'title = %(title)s' if title else ''}
                      where id = %(thread_id)s returning *'''
        cur.execute(sql, {'thread_id': thread['id'], 'message': message, 'title': title})
        return replace_time_format(cur.fetchone())


def create_vote(thread, user, voice):
    with get_DB_cursor() as cur:
        user_id = user['id']
        thread_id = thread['id']
        sql = 'select voice from votes where user_id = %s and thread_id = %s'
        cur.execute(sql, (user_id, thread_id))
        result = cur.fetchone()
        old_voice = result['voice'] if result else None
        if old_voice is not None:
            delta = voice - old_voice
            if delta != 0:
                sql = '''--select from threads where id = %(thread_id)s for update;
                         update threads set votes = votes + %(delta)s where id = %(thread_id)s;
                         update votes set voice = %(voice)s where user_id = %(user_id)s and thread_id = %(thread_id)s'''
                cur.execute(sql, {'delta': delta, 'thread_id': thread_id, 'voice': voice, 'user_id': user_id})
        else:
            sql = '''insert into votes (thread_id, user_id, voice) values (%s, %s, %s);
                     update threads set votes = votes + %s where id = %s'''
            cur.execute(sql, (thread_id, user_id, voice, voice, thread_id))


def get_posts(thread, limit=None, since=None, sort=None, is_desc=False):
    sort = sort or 'flat'
    select_from_sql = '''
        select
            p.user_nickname as author,
            p.created,
            p.forum_slug as forum,
            p.id,
            p.is_edited as isEdited,
            p.message,
            p.parent_id as parent,
            p.thread_id as thread
        from posts as p
        '''
    desc_sql = 'desc' if is_desc else ''
    limit_sql = 'limit %(limit)s' if limit else ''
    compare_sign = '<' if is_desc else '>'
    compare_sign_reverce = '>' if is_desc else '<'
    since_sql = f'''and parent_path {compare_sign} (select parent_path
                                                      from posts where id = {since})''' if since else ''
    since_sql_reverce = f'''and ((select parent_path
                                 from posts where id = {since})''' if since else ''

    with get_DB_cursor() as cur:
        if sort == 'flat':
            sql = f'''
                {select_from_sql}
                where p.thread_id = %(thread_id)s
                {f'and p.id {compare_sign} %(since)s' if since else ''}
                order by p.created {desc_sql},
                    p.id {desc_sql}
                {limit_sql}
                '''

        elif sort == 'tree':
            sql = f'''
                {select_from_sql}
                where p.thread_id = %(thread_id)s
                {since_sql}
                order by p.parent_path {desc_sql}, p.id {desc_sql}
                {limit_sql}
                '''

        elif sort == 'parent_tree':
            sql = f'''
                select p2.user_nickname as author,
            p2.created,
            p2.forum_slug as forum,
            p2.id,
            p2.is_edited as isEdited,
            p2.message,
            p2.parent_id as parent,
            p2.thread_id as thread
                from (select id from posts where thread_id = %(thread_id)s and parent_id = 0
                      {since_sql}
                      order by id {desc_sql} {limit_sql}) as p1
                    inner join (select * from posts as p2 where p2.parent_path[2] = any(select id from posts)) as p2 on p1.id = p2.parent_path[2]
                order by p2.parent_path {desc_sql}
                '''

        cur.execute(sql, {'thread_id': thread['id'], 'since': since, 'limit': limit})
        return list(map(replace_time_format, cur.fetchall()))


@forum_blueprint.route('/forum/create', methods=['POST'])
def _create_forum():
    data = request.get_json()

    user = get_user_or_404(nickname=data['user'])
    forum = get_forum_or_404(slug_or_id=data['slug'], abort404=False)

    status = CONFLICT
    if not forum:
        status = CREATED
        forum = create_forum(user['id'], data['title'], data['slug'])

    return make_response(status, get_forum_info(forum, user))


@forum_blueprint.route('/forum/<slug>/create', methods=['POST'])
def _create_thread(slug):
    data = request.get_json()

    thread_slug = data.get('slug')

    forum = get_forum_or_404(slug_or_id=slug)
    thread = get_thread_or_404(forum_id=forum['id'], slug_or_id=thread_slug, abort404=False) if thread_slug else None
    user = get_user_or_404(user_id=thread['user_id']) if thread is not None else get_user_or_404(nickname=data['author'])
    if thread is not None:
        forum = get_forum_or_404(slug_or_id=thread['forum_id'])

    status = CONFLICT
    if thread is None:
        status = CREATED
        thread = create_thread(user, forum, data['title'], data['message'],
                               slug=thread_slug if thread_slug != '0' else '', created=data.get('created'))

    with_slug = data.get('slug') is not None
    return make_response(status, get_thread_info(thread, forum, user, with_slug=with_slug))


@forum_blueprint.route('/forum/<slug>/details')
def get_forum_details(slug):
    forum = get_forum_or_404(slug_or_id=slug)
    user = get_user_or_404(user_id=forum['user_id'])
    return make_response(OK, get_forum_info(forum, user))


@forum_blueprint.route('/forum/<slug>/threads')
def get_forum_threads(slug):
    get_forum_or_404(slug_or_id=slug)
    limit = request.args.get('limit')
    since = request.args.get('since')
    desc = request.args.get('desc')
    threads = get_threads_info(slug, limit, since, desc)
    return make_response(OK, threads)


@forum_blueprint.route('/forum/<slug>/users')
def get_thread_users(slug):
    forum = get_forum_or_404(slug_or_id=slug)
    limit = request.args.get('limit')
    since = request.args.get('since')
    desc = request.args.get('desc') == 'true'
    users = get_forum_users_info(forum['id'], limit, since, desc)
    return make_response(OK, users)


@forum_blueprint.route('/post/<p_id>/details', methods=['GET'])
def get_post_details(p_id):
    post = get_post_or_404(post_id=p_id)
    what_to_show = request.args.get('related')
    what_to_show = what_to_show.split(',') if what_to_show is not None else None
    posts = get_post_info(p_id, what_to_show, thread_as_id=True)
    return make_response(OK, posts)


@forum_blueprint.route('/post/<p_id>/details', methods=['POST'])
def change_post_details(p_id):
    post = get_post_or_404(post_id=p_id)
    data = request.get_json()
    return make_response(OK, change_post_message(p_id, data.get('message')))


@forum_blueprint.route('/service/clear', methods=['POST'])
def clear_forum():
    drop_forum()
    return make_response(OK, 'Forum database cleared')


@forum_blueprint.route('/service/status')
def get_status():
    return make_response(OK, get_forum_status())


@forum_blueprint.route('/thread/<slug_or_id>/create', methods=['POST'])
def create_posts(slug_or_id):
    data = request.get_json()
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    forum = get_forum_or_404(slug_or_id=thread['forum_id'])
    posts = add_posts(thread['id'], data, forum_id=thread['forum_id'], forum_slug=forum['slug'])
    posts = [get_post_info(p['id'], thread_as_id=True)['post'] for p in posts]
    return make_response(CREATED, posts)


@forum_blueprint.route('/thread/<slug_or_id>/details', methods=['GET'])
def _get_thread_details(slug_or_id):
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    return make_response(OK, get_thread_info(thread))


@forum_blueprint.route('/thread/<slug_or_id>/details', methods=['POST'])
def _change_thread(slug_or_id):
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    data = request.get_json()
    new_thread = change_thread(thread, data.get('message'), data.get('title'))
    return make_response(OK, get_thread_info(new_thread))


@forum_blueprint.route('/thread/<slug_or_id>/posts', methods=['GET'])
def _get_posts(slug_or_id):
    limit = request.args.get('limit')
    since = request.args.get('since')
    sort = request.args.get('sort')
    desc = request.args.get('desc')

    thread = get_thread_or_404(slug_or_id=slug_or_id)
    posts = get_posts(thread, limit, since, sort, desc == 'true')

    return make_response(OK, posts)

    pass


@forum_blueprint.route('/thread/<slug_or_id>/vote', methods=['POST'])
def _change_vote(slug_or_id):
    data = request.get_json()
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    user = get_user_or_404(nickname=data['nickname'])
    create_vote(thread, user, int(data['voice']))
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    return make_response(OK, get_thread_info(thread))


@forum_blueprint.route('/user/<nickname>/create', methods=['POST'])
def _create_user(nickname):
    data = request.get_json()

    try:
        user = get_user_info_or_404(nickname=nickname, email=data['email'], in_list=True, with_nickname=True)
        return make_response(CONFLICT, user)
    except:
        pass

    user = create_user(nickname, data['fullname'], data['email'], data['about'])

    return make_response(CREATED, get_user_info_or_404(nickname=user['nickname'],
                                                      email=user['email'],
                                                      with_nickname=True))


@forum_blueprint.route('/user/<nickname>/profile', methods=['GET'])
def _get_user_info(nickname):
    return make_response(OK, get_user_info_or_404(nickname=nickname, with_nickname=True))


@forum_blueprint.route('/user/<nickname>/profile', methods=['POST'])
def _change_user(nickname):
    data = request.get_json()

    user = get_user_or_404(nickname=nickname)
    if data.get('email') and get_user_or_404(email=data['email'], abort404=False):
        return make_response(CONFLICT, {'message': data['email']})
    else:
        user = change_user(nickname, about=data.get('about'), email=data.get('email'), fullname=data.get('fullname'))
        return make_response(OK, user)
