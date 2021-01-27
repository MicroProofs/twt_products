from pony.orm import *

db = Database()

# PostgreSQL
db.bind(provider='postgres', user='', password='', host='', database='')