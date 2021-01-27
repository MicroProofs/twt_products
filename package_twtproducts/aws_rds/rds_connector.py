import psycopg2
import sys
from psycopg2.extras import execute_values



# cur.execute("ALTER SEQUENCE ProductLinks_id_seq RESTART WITH 1;")
# cur.execute("ALTER SEQUENCE HashTags_id_seq RESTART WITH 1;")
# cur.execute(
#     "ALTER TABLE Batches ALTER COLUMN created_on SET DEFAULT now();")

def insertor(links, hashtagsWords, kwords):
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
        cur.execute(
            "INSERT INTO Batches (keywords) VALUES (%s) RETURNING *", (kwords,))
        rows = cur.fetchall()
        print(rows[0])
        batch_numb = rows[0][0]
        batch_date = rows[0][2]
        links_tuple = [(k, v, batch_date, batch_numb)
                       for k, v in links.items()]
        hashtag_tuple = [(k, v, batch_date, batch_numb)
                         for k, v in hashtagsWords.items()]
        execute_values(
            cur, "INSERT INTO ProductLinks (web_link, occurence, batch_date, batch_number) VALUES %s", links_tuple)
        execute_values(
            cur, "INSERT INTO HashTags (hashtag, occurence, batch_date, batch_number) VALUES %s", hashtag_tuple)

        # cur.execute("SELECT * FROM ProductLinks")
        # [print(cn) for cn in cur.description]
        # rows = cur.fetchall()
        # print(rows)
        # cur.execute("SELECT * FROM HashTags")
        # rows = cur.fetchall()
        # [print(cn) for cn in cur.description]
        # print(rows)
        cur.execute("SELECT * FROM Batches ORDER BY batch_number DESC LIMIT 5")
        rows = cur.fetchall()
        [print(cn) for cn in cur.description]
        print(rows)
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print(f'Error {e}')
        sys.exit(1)

    finally:
        if con:
            con.close()


