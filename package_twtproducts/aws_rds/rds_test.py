import psycopg2
import sys
from psycopg2.extras import execute_values


# cur.execute("ALTER SEQUENCE ProductLinks_id_seq RESTART WITH 1;")
# cur.execute("ALTER SEQUENCE HashTags_id_seq RESTART WITH 1;")

# cur.execute(
#     "INSERT INTO Batches (keywords) VALUES (%s) RETURNING *", (["fdsf", 'afdsafd'],))

# rows = cur.fetchall()
# print(rows[0])
# batch_numb = rows[0][0]
# batch_date = rows[0][2]
# links_tuple = [(k, v, batch_date, batch_numb)
#                for k, v in {"fdasfdas": 3}.items()]
# hashtag_tuple = [(k, v, batch_date, batch_numb)
#                  for k, v in {"dasfdfda": 3}.items()]
# execute_values(
#     cur, "INSERT INTO ProductLinks (web_link, occurence, batch_date, batch_number) VALUES %s", links_tuple)
# execute_values(
#     cur, "INSERT INTO HashTags (hashtag, occurence, batch_date, batch_number) VALUES %s", hashtag_tuple)

con = None
try:
    con = psycopg2.connect(
        database="bigStore",
        user="postgres",
        password="",
        host="database-1.cmryc5m9nrth.us-east-1.rds.amazonaws.com",
        port='5432'
    )

    cur = con.cursor()
    # row[3].strftime("%m/%d/%Y, %H:%M:%S")

    # cur.execute("SELECT web_link, SUM(occurence) AS total_occurrence FROM ProductLinks GROUP BY web_link ORDER BY total_occurrence DESC")
    # [print(cn) for cn in cur.description]
    # print()
    # rows = cur.fetchall()
    # for row in rows:
    #     if(row[1] > 1):
    #         print("link " + row[0] + " | occurrence " + str(row[1]))

    # cur.execute(
    #     "SELECT * FROM ProductLinks ORDER BY batch_number DESC;")
    # [print(cn) for cn in cur.description]
    # print()
    # rows = cur.fetchall()
    # for row in rows:
    #     print("id: " + str(row[0]) + " link: " +
    #           row[1] + " occurrence: " + str(row[2]))

    # where web_link LIKE '%amz%' OR web_link LIKE '%amaz%' OR web_link LIKE '%bay%'

    cur.execute(
        "SELECT * FROM (SELECT web_link, SUM(occurence) AS total_occurence FROM ProductLinks WHERE batch_number > 3000 GROUP BY web_link ORDER BY total_occurence) AS foo")
    [print(cn) for cn in cur.description]
    print()
    rows = cur.fetchall()
    for row in rows:
        print("link: " + str(row[0]) + " occurrence: " + str(row[1]))

    # cur.execute("SELECT hashtag, SUM(occurence) AS total_occurence FROM HashTags WHERE batch_number > 400 GROUP BY hashtag ORDER BY total_occurence")
    # [print(cn) for cn in cur.description]
    # print()
    # rows = cur.fetchall()
    # for row in rows:
    #     print(row)

    # cur.execute("SELECT hashtag, SUM(occurence) AS total_occurrence FROM HashTags GROUP BY hashtag ORDER BY total_occurrence DESC")
    # [print(cn) for cn in cur.description]
    # print()
    # rows = cur.fetchall()
    # for row in rows:
    #     if(row[1] > 2):
    #         print("hashtag " + row[0] + " | occurrence " + str(row[1]))

    # cur.execute(
    #     "SELECT * FROM HashTags WHERE occurence > 2 ORDER BY occurence DESC")
    # rows = cur.fetchall()
    # [print(cn) for cn in cur.description]
    # print()
    # for row in rows:
    #     print("hashtag " + row[1] + " | occurrence " + str(row[2]
    #                                                        ) + " | " + row[3].strftime("%m/%d/%Y, %H:%M:%S"))

    # cur.execute("ALTER SEQUENCE Batches_batch_number_seq RESTART WITH 46;")
    # cur.execute("SELECT last_value FROM Batches_batch_number_seq")
    # rows = cur.fetchall()
    # print(rows)

    # cur.execute("DELETE FROM Hashtags WHERE batch_number > 45")
    cur.execute("SELECT * FROM Batches ORDER BY batch_number DESC LIMIT 2")
    rows = cur.fetchall()
    [print(cn) for cn in cur.description]
    print()
    for row in rows:
        print("batch number " + str(row[0]) + " | keywords " +
              str(row[1]) + " | " + row[2].strftime("%m/%d/%Y, %H:%M:%S"))

    # con.commit()
except psycopg2.DatabaseError as e:
    if con:
        con.rollback()
    print(f'Error {e}')
    sys.exit(1)

finally:
    if con:
        con.close()
