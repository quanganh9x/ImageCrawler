from flask import current_app, g
from sqlalchemy import engine, create_engine


def init_app(app):
    app.teardown_appcontext(close_db)


def get_db():
    if 'db' not in g:
        g.db = create_engine('mysql://root:Nam123456@localhost/imagecrawler?charset=utf8mb4').raw_connection()

    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()
