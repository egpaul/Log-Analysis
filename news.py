#!/usr/bin/env python3


import psycopg2
DBNAME = 'news'

# Connects to the database and executes the queries in sql


def data(query):
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    results = c.fetchall()
    db.close()
    return(results)

# Creates the sql query requirements and runs the response for Q1


def PopularArticles():
    question_1 = ("What are the most popular three articles of all time?")
    query_1 = '''select articles.title, count(*) as views from articles
    inner join log on log.path = concat('/article/', articles.slug)
    group by log.path, articles.title order by views desc limit 3;'''

    run_report = data(query_1)
    print("    " + question_1)
    for i in range(len(run_report)):
        print(' {}: Article: {}, Total Views: {}'
              .format(i + 1, run_report[i][0], run_report[i][1]))


# Creates the sql query requirements and runs the response for Q2
def PopularAuthors():
    question_2 = "Who are the most popular article authors of all time?"
    query_2 = '''select authors.name, count(*) as views from articles
    inner join authors on authors.id=articles.author inner join
    log on log.path = concat('/article/', articles.slug)
    group by authors.name order by views desc;'''

    run_report = data(query_2)
    print("    " + question_2)
    for i in range(len(run_report)):
        print(' {}: Author: {}, Total Views: {}'
              .format(i + 1, run_report[i][0], run_report[i][1]))


# Creates the sql query requirements and runs the response for Q3
def RequestErrors():
    question_3 = "On which days did more than 1% of requests lead to errors?"
    query_3 = '''select * from (select date, CAST((CAST(errorRequest AS float)
    * 100.0 ) / CAST(totalRequest AS float) as DECIMAL(16,2)) as
    errorpercentage from (select T.date, totalRequest, errorRequest from (
    select date(time) as date, count(*) as errorRequest from log where
    log.status like '%404%' group by date) as T inner join(select date(time) as
    date, count(*) as totalRequest from log group by date) as E on
    T.date=E.date) as logtable) as errorRequest where errorpercentage >1.0;'''

    run_report = data(query_3)
    print("    " + question_3)
    for i in range(len(run_report)):
        print(' {}: Date: {}, Percent Error: {}'
              .format(i + 1, run_report[i][0], run_report[i][1]))


# Executes the python programming
if __name__ == "__main__":
    PopularArticles()
    print('\n')
    PopularAuthors()
    print('\n')
    RequestErrors()
