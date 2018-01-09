import psycopg2


def Data(query):
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    results = c.fetchall()
    db.close()
    return(results)

# 1. What are the three most popular articles of all time?


def PopularArticles():
    query = '''select articles.title, count (*) as views from articles inner
    join log on log.path like concat (%, articles.slug, '%') where log.status
    like '%200' group by articles.title, log.path order by views desc limit
    3;'''

    # 2. Who are the most popular article authors of all time?


def PopularAuthors():
    query = '''select authors.name, count (*) as views from articles inner
    join authors on articles.author = authors.id inner join log on log.path
    = concat ('/article/', articles.slug) group by authors.name order by
    views.desc'''

    # 3. On whih days did more than 1% of requests lead to errors?


def RequestErrors():
    query = '''select day, perc from ('select day, round (sum(requests)/
    (select count (*) from log where substring(cast(log.time as text), 0, 11
    = day) * 100), 2) as perc from (select substring(cast(log.time as text)
    0, 11) as day, count (*) as requests from log where status like '%404'
    group by day) as log_percentage group by day order by perc desc) as
    final_query where perc >= 1%)'''
