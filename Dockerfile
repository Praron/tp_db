FROM ubuntu:16.04

RUN apt-get -y update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get -y update
RUN apt-get install -y python3.6 python3-pip postgresql-contrib-9.5

ADD requirements.txt ./forum/
ADD *.py ./forum/
ADD db_init.sql ./
ADD db_init.sql ./forum/

RUN /usr/bin/python3.6 -m pip install -r forum/requirements.txt

RUN echo "listen_addresses='*'" >> /etc/postgresql/9.5/main/postgresql.conf &&\
    echo "fsync = 'off'" >> /etc/postgresql/9.5/main/postgresql.conf &&\
    echo "synchronous_commit=off" >> /etc/postgresql/9.5/main/postgresql.conf &&\
    echo "shared_buffers = 256MB" >> /etc/postgresql/9.5/main/postgresql.conf &&\
    echo "autovacuum = off" >> /etc/postgresql/9.5/main/postgresql.conf

USER postgres
RUN /etc/init.d/postgresql start &&\
    psql --command "CREATE USER admin WITH SUPERUSER PASSWORD 'admin';" &&\
    createdb -E UTF8 -T template0 forum &&\
    psql forum --command "CREATE EXTENSION citext;" &&\
    psql forum -a -f db_init.sql &&\
    /etc/init.d/postgresql stop

USER root
EXPOSE 5000
CMD /etc/init.d/postgresql start &&\
    cd forum &&\
    gunicorn -w 8 -b 0.0.0.0:5000 main:app
