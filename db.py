#!/usr/bin/env python2.7

import psycopg2


article_views = open('article_views.txt', 'w+')
author_views = open('author_views.txt', 'w+')
errors = open('errors.txt', 'w+')


def connect():
    return psycopg2.connect("dbname=news")
    

def stat1():
    d_b = connect()
    cur = d_b.cursor()
    cur.execute("""select count(*) as cnt,articles.title
        from log,articles where
        log.path='/article/'||articles.slug
        group by articles.title
        order by cnt desc limit 3;""")
    count = cur.fetchall()
    for item in count:
        article_views.write('"'+str(item[1])+'"'+' - '+str(item[0])+" views\n")
    d_b.close()


def stat2():
    d_b = connect()
    cur = d_b.cursor()
    cur.execute("""select count(*) as cnt, authors.name
        from log,articles,authors
        where authors.id = articles.author and
        log.path = '/article/'||articles.slug
        group by authors.name
        order by cnt desc;""")
    count = cur.fetchall()
    for item in count:
        author_views.write(str(item[1])+' - '+str(item[0])+" views\n")
    d_b.close()


def stat3():
    d_b = connect()
    cur = d_b.cursor()
    cur.execute("""create view total as
        select count(*) as total,date(time)
        from log
        group by date(time)
        order by date(time);""")
    cur.execute("""create view errors as
        select count(*) as errors,date(time)
        from log
        where status like '4%'
        group by date(time)
        order by date(time);""")
    cur.execute("""create view percentage as
        select (cast(errors.errors as float)/cast(total.total as float))*100
        as percentage,total.date
        from errors natural join total;""")
    cur.execute("""select * from percentage
        where percentage>1;""")
    count = cur.fetchall()
    for item in count:
        errors.write(str(item[1])+' - '+"%.2f" % item[0]+"% errors\n")
    d_b.close()

stat1()
stat2()
stat3()
