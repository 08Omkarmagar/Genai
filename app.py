import os
import psycopg2
from flask import Flask, render_template
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

DB_HOST=os.getenv("DB_HOST", "localhost")
DB_PORT=os.getenv("DB_PORT", "5432")
DB_NAME=os.getenv("DB_NAME", "newsdb")
DB_USER=os.getenv("DB_USER", "postgres")
DB_PASSWORD=os.getenv("DB_PASSWORD")


def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn


@app.route("/")
def index():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM rss_articles")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("index.html", rows=rows)


if __name__ == "__main__":
    app.run(debug=True)