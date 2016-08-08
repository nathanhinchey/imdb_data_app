import sqlite3

db = sqlite3.connect(':memory:')
# using a sqlite3 db in memory as a stand in for the AWS db

def genre_list(movie_vals):
    return movie_vals[-1][2:-2].split("', '")

def init_db(cur):
    cur.execute('''CREATE TABLE movies (
        title text,
        year integer,
        rating real,
        votes integer,
        genres text)''')
    cur.execute('''CREATE TABLE genres (genre text)''')
    cur.execute('''CREATE TABLE movie_genres (
        genre_id integer,
        movie_id integer)''')

def assign_genres(movie_vals, movie_id):
    genre_list = movie_vals[-1][2:-2].split("', '")
    #convert the json-like from the data set list to a python list
    for genre in genre_list:
        genre_id = cur.execute('''
            SELECT rowid FROM genres WHERE genre = ?''',(genre,)).fetchone()
        if genre_id == None:
            # this could be avoided by knowing a full list of genres
            # in advance, but it's relatively cheap anyway
            cur.execute('''INSERT INTO genres (genre) VALUES (?)''', (genre,))
            genre_id = (cur.lastrowid,)
        cur.execute('''INSERT INTO movie_genres (genre_id, movie_id)
            VALUES (?,?)''', (genre_id[0],movie_id))

def populate_db(cur,source):
    with open(source,'r',encoding='ISO-8859-1') as movie_file:
        for movie in movie_file:
            movie_vals = movie.strip().split(' +++$+++ ')[1:]
            cur.execute('''
                INSERT INTO movies (title, year, rating, votes)
                VALUES (?,?,?,?)''', tuple(movie_vals[:-1]))
            assign_genres(movie_vals, cur.lastrowid)
            

cur = db.cursor()
init_db(cur)
populate_db(cur,'movie_titles_metadata.txt')
db.commit()
