from sys import stderr
import pytz
import re
from datetime import datetime, timedelta
from contextlib import contextmanager
import numbers
from flask import Blueprint, request, Response, json, abort
from connect_db import connect_DB
import psycopg2
import psycopg2.extras

forum_blueprint = Blueprint('forum', __name__)

CONFLICT = 409
CREATED = 201
OK = 200

@contextmanager
def get_DB_cursor():
    conn = connect_DB()  # get from pool
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
        conn.close()  # return to pool


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
                # {'forum_id = %(forum_id)s and' if forum_id is not None else ''}
                # {'id = %(slug_or_id)s' if is_number(slug_or_id) else 'lower(slug) = lower(%(slug_or_id)s)'}'''
            sql = f'''select * from threads where
                {'id' if is_number(slug_or_id) else 'slug'} = %(slug_or_id)s'''
            cur.execute(sql, {'forum_id': forum_id, 'slug_or_id': slug_or_id})
            thread = cur.fetchone()
            print(cur.mogrify(sql, {'forum_id': forum_id, 'slug_or_id': slug_or_id}).decode('utf-8'), file=stderr)
            if thread is not None:
                thread = replace_time_format(thread)
            return (thread or abort(404, slug_or_id)) if abort404 else thread
    raise Exception('No argument!')


def get_parent_or_409(parent_id=None):
    if parent_id is not None:
        with get_DB_cursor() as cur:
            sql = 'select * from posts where id = %s'
            cur.execute(sql, (parent_id,))
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
    # zone = pytz.timezone('Europe/Moscow')
    # if time.tzinfo is None:
    #     time = zone.localize(time)
    utc_time = time.astimezone(pytz.utc)
    row['created'] = utc_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return row


def create_thread(user_id, forum_id, title, message, slug=None, created=None):
    with get_DB_cursor() as cur:
        created = created or str(datetime.now())
        # cur.execute("SET TIME ZONE 'Europe/Moscow';")
        sql = f'''insert into threads (user_id, forum_id, title, message, created
                  {', slug' if slug else ''}) 
                  values (%(user_id)s, %(forum_id)s, %(title)s, %(message)s, %(created)s {', %(slug)s' if slug else ''})
                  returning *'''

        args = {'user_id': user_id, 'forum_id': forum_id, 'title': title, 'slug': slug, 'message': message, 'created': created}
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
    info = get_dict_part(forum, ['slug', 'posts', 'title', 'threads'])
    info['user'] = user['nickname']
    info['forum'] = info['slug']
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
            ms = since[-4:]
            since = datetime.strptime(since[:-4], '%Y-%m-%d %H:%M:%S') + timedelta(hours=3)
            since = format(since, '%Y-%m-%d %H:%M:%S')
            since += ms

        sql = f'''
            select
                u.nickname as author,
                t.created,
                f.slug as forum,
                t.id,
                t.message,
                t.slug,
                t.title,
                t.votes
            from threads as t
                join users as u on u.id = t.user_id
                join forums as f on f.id = t.forum_id
            where f.slug = %(forum)s
            {f"and t.created {'<=' if is_desc else '>='} %(since)s" if since else ''}
            order by t.created {'desc' if is_desc else ''}
            {f'limit %(limit)s' if limit else ''}
            '''
        cur.execute(sql, {'forum': forum_name, 'since': since, 'limit': limit})
        threads = cur.fetchall()
        return list(map(replace_time_format, threads))


def get_forum_users_info(forum_name, limit=None, since=None, is_desc=False):
    with get_DB_cursor() as cur:
        sql = f'''
            select distinct on (u.nickname)
                u.nickname,
                u.fullname,
                u.email,
                u.about
            from users as u
                left join threads as t on t.user_id = u.id
                left join posts as p on p.user_id = u.id
            {f'where u.id >= %(since)s' if since else ''}
            order by u.nickname {'desc' if is_desc else ''}
            {f'limit %(limit)s' if limit else ''}
            '''
        cur.execute(sql, {'forum': forum_name, 'since': since, 'limit': limit})
        return cur.fetchall()


def change_post_message(post_id, message):
    with get_DB_cursor() as cur:
        sql = '''update posts set message = '%s', is_edited = true where id = %s returning *'''
        cur.execute(sql, message, post_id)
        return cur.fetchone()


def get_post_info(post_id, what_to_show=None, thread_as_id=False):
    what_to_show = what_to_show or []
    is_author_need = 'author' in what_to_show
    is_thread_need = 'thread' in what_to_show
    is_forum_need = 'forum' in what_to_show
    answer = {}
    with get_DB_cursor() as cur:
        sql = f'''
            select u.nickname as author,
            p.created,
            f.slug as forum,
            p.id,
            p.is_edited,
            p.message,
            p.parent_id,
            {'t.id' if thread_as_id else 't.slug'} as thread,
            t.id as thread_id
            from posts as p
                join users as u on p.user_id = u.id
                join threads as t on p.thread_id = t.id
                join forums as f on t.forum_id = f.id
            where p.id = %s
            '''
        cur.execute(sql, (post_id,))
        post = cur.fetchone()

        answer['post'] = post

        forum = None
        user = None
        if is_author_need:
            user = user or get_user_or_404(nickname=post['author'])
            answer['user'] = user
        if is_thread_need:
            user = user or get_user_or_404(nickname=post['author'])
            forum = forum or get_forum_or_404(slug_or_id=post['forum'])
            thread = get_thread_or_404(forum_id=forum['id'], slug_or_id=(post['thread'] or post['thread_id']))
            answer['thread'] = get_thread_info(thread, forum, user)
        if is_forum_need:
            user = user or get_user_or_404(nickname=post['author'])
            forum = forum or get_forum_or_404(slug_or_id=post['forum'])
            answer['forum'] = get_forum_info(forum, user)

        post['thread'] = post['thread'] or post['thread_id']
        del post['thread_id']

        return answer


def drop_forum():
    with open('db_init.sql', 'r') as sql:
        with get_DB_cursor() as cur:
            cur.execute(sql.read().replace('\n', ' '))


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


def add_posts(thread_id, posts):
    time = str(datetime.now())
    with get_DB_cursor() as cur:
        sql = f'''insert into posts (user_id, thread_id, message, created, parent_id) values
             (%s, %s, %s, %s, %s){', (%s, %s, %s, %s, %s)' * (len(posts) - 1)} returning *'''
        args = []
        if len(posts) == 0:
            return []
        for post in posts:
            user = get_user_or_404(nickname=post['author'])
            if post.get('parent'):
                get_parent_or_409(parent_id=post['parent'])
            args += (user['id'], thread_id, post['message'], time, post.get('parent', 0))
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
        sql = f'''update threads set {'message = %(message)s' if message else ''}
                                     {',' if message and title else ''}
                                     {'title = %(title)s' if title else ''}
                  where id = %s'''
        cur.execute(sql, {'message': message, 'title': title})
        return cur.fetchone()


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
                sql = 'update threads set votes = votes + %s where id = %s'
                cur.execute(sql, (delta, thread_id))
                sql = 'update votes set voice = %s where user_id = %s and thread_id = %s'
                cur.execute(sql, (voice, user_id, thread_id))
        else:
            sql = 'insert into votes (thread_id, user_id, voice) values (%s, %s, %s)'
            cur.execute(sql, (thread_id, user_id, voice))


def get_posts(thread, limit=None, since=None, sort=None, is_desc=False):
    sort = sort or 'flat'
    select_sql = '''
        select
            u.nickname as author,
            p.created,
            f.title as forum,
            p.id,
            p.is_edited as isEdited,
            p.message,
            p.parent_id as parent,
            p.thread_id as thread
        '''
    from_sql = '''
        from posts as p
            left join users as u on p.user_id = u.id
            left join threads as t on p.thread_id = t.id
            left join forums as f on t.forum_id = f.id
        '''
    desc_sql = 'desc' if is_desc else ''
    limit_sql = 'limit %(limit)s' if limit else ''
    compare_sign = '<' if is_desc else '>'
    since_sql = f'''and p.parent_path {compare_sign} (select parent_path
                                                      from posts where id = {since})''' if since else ''

    with get_DB_cursor() as cur:
        if sort == 'flat':
            sql = f'''
                {select_sql}
                {from_sql}
                where p.thread_id = %(thread_id)s
                {f'and p.id >= %(since)s' if since else ''}
                order by p.created {desc_sql},
                    p.id {desc_sql}
                {limit_sql}
                '''


        elif sort == 'tree':
            sql = f'''
                {select_sql}
                {from_sql}
                where p.thread_id = %(thread_id)s
                {since_sql}
                order by p.parent_path {desc_sql}, p.id {desc_sql}
                {limit_sql}
                '''
        
        elif sort == 'parent_tree':
            sql = f'''
                {select_sql}
                {from_sql}
                where parent_path[2] in (select id from posts where thread_id = %(thread_id)s and parent_id = 0
                {since_sql}
                order by id {desc_sql}
                {limit_sql}
                ) order by parent_path {desc_sql}
                '''
        cur.execute(sql, {'thread_id': thread['id'], 'since': since, 'limit': limit})
        return cur.fetchall()


    pass


@forum_blueprint.route('/forum/create', methods=['POST'])
def _create_forum():
    # data = {'user': 'shrek', 'slug': 'first-init', 'title': 'Title'}
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
    # data = {'author': 'shrek', 'created': '2017-01-01T00:00:00.000Z', 'title': 'Hello', 'message': 'World', 'slug': 'threadslug3'}
    data = request.get_json()

    thread_slug = data.get('slug')

    forum = get_forum_or_404(slug_or_id=slug)
    thread = get_thread_or_404(forum_id=forum['id'], slug_or_id=thread_slug, abort404=False) if thread_slug else None
    user = get_user_or_404(id=thread['user_id']) if thread is not None else get_user_or_404(nickname=data['author'])

    status = CONFLICT
    if thread is None:
        status = CREATED
        thread = create_thread(user['id'], forum['id'], data['title'], data['message'],
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
    get_forum_or_404(slug_or_id=slug)
    limit = request.args.get('limit')
    since = request.args.get('since')
    desc = request.args.get('desc')
    users = get_forum_users_info(slug, limit, since, desc)
    return make_response(OK, users)


@forum_blueprint.route('/post/<p_id>/details', methods=['GET'])
def get_post_details(p_id):
    post = get_post_or_404(post_id=p_id)
    what_to_show = request.args.getlist('related')
    what_to_show = ['forum', 'author', 'thread']
    posts = get_post_info(p_id, what_to_show)
    return make_response(OK, posts)


@forum_blueprint.route('/post/<p_id>/details', methods=['POST'])
def change_post_details(p_id):
    post = get_post_or_404(post_id=p_id)
    data = request.get_json()
    return make_response(OK, change_post_message(p_id, data['message']))


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
    # data = '''[
    #       {
    #         "author": "shrek",
    #         "message": "We should be afraid of the Kraken.",
    #         "parent": 0
    #       },
    #       {
    #         "author": "shrek",
    #         "message": "We shouldn't be afraid of the Kraken.",
    #         "parent": 0
    #       }
    #     ]'''
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    posts = add_posts(thread['id'], data)
    posts = [replace_time_format(get_post_info(p['id'], thread_as_id=True)['post']) for p in posts]
    return make_response(CREATED, posts)
    # return make_response(CREATED, list(map(replace_time_format, posts)))


@forum_blueprint.route('/thread/<slug_or_id>/details', methods=['GET'])
def _get_thread_details(slug_or_id):
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    return make_response(OK, get_thread_info(thread))


@forum_blueprint.route('/thread/<slug_or_id>/details', methods=['POST'])
def _change_thread(slug_or_id):
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    data = request.get_json()
    # data = {
    #     'message': 'An urgent need to reveal the hiding place of Davy Jones. Who is willing to help in this matter?',
    #     'title': 'Davy Jones cache'
    #     }
    new_thread = change_thread(thread, data.get('message'), data.get('title'))
    return make_response(OK, new_thread)


@forum_blueprint.route('/thread/<slug_or_id>/posts', methods=['GET'])
def _get_posts(slug_or_id):
    limit = request.args.get('limit')
    since = request.args.get('since')
    sort = request.args.get('sort')
    desc = request.args.get('desc')

    thread = get_thread_or_404(slug_or_id=slug_or_id)
    posts = get_posts(thread, limit, since, sort, desc == 'true')

    # return make_response(OK, 'hello')
    return make_response(OK, posts)

    pass


@forum_blueprint.route('/thread/<slug_or_id>/vote', methods=['POST'])
def _change_vote(slug_or_id):
    data = request.get_json()
    # data = {'nickname': 'shrek', 'voice': '0'}
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    user = get_user_or_404(nickname=data['nickname'])
    create_vote(thread, user, int(data['voice']))
    thread = get_thread_or_404(slug_or_id=slug_or_id)
    return make_response(OK, get_thread_info(thread))


@forum_blueprint.route('/user/<nickname>/create', methods=['POST'])
def _create_user(nickname):
    data = request.get_json()
    # data = {
    #     'about': 'This is the day you will always remember as the day that you almost caught Captain Jack Sparrow!',
    #     'email': 'captaina@blackpearl.sea',
    #     'fullname': 'Captain Jack Sparrow'
    #     }

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
    # data = {
    #     'about': 'This is the day you will always remember as the day that you almost caught Captain Jack Sparrow!',
    #     'email': 'captaina@blackpearl.sea',
    #     'fullname': 'Captain Jack Sparrow'
    #     }

    user = get_user_or_404(nickname=nickname)
    if data.get('email') and get_user_or_404(email=data['email'], abort404=False):
        return make_response(CONFLICT, {'message': data['email']})
    else:
        user = change_user(nickname, about=data.get('about'), email=data.get('email'), fullname=data.get('fullname'))
        return make_response(OK, user)
