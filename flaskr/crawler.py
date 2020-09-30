
from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for, make_response, jsonify
)
from sqlalchemy import create_engine
#import file from another source
from flaskr.db import get_db

from kaggle.api.kaggle_api_extended import KaggleApi

from pathlib import Path
import logging
import pandas

bp = Blueprint('crawler', __name__)

download_dir = Path().absolute().joinpath('download')
data_dir = Path().absolute().joinpath('flaskr').joinpath('static').joinpath('data')


def import_box(url):
    return False


def import_github(url):
    return False


def import_kaggle(url):
    api = KaggleApi()
    api.authenticate()

    try:
        api.dataset_download_files(url, download_dir, unzip=True)
        return True
    except ValueError:
        logging.error("Specified dataset is not supported")
        return False
    except OSError:
        logging.error("An error occurred")
        return False


def move_files(prefix):
    if not data_dir.exists():
        data_dir.mkdir()

    csv_files = download_dir.rglob('*.csv')
    csv_names = []

    image_files = []
    image_files.extend(download_dir.rglob('*.jpg'))
    image_files.extend(download_dir.rglob('*.png'))
    image_files.extend(download_dir.rglob('*.jpeg'))

    try:
        if not data_dir.joinpath(prefix).exists():
            data_dir.joinpath(prefix).mkdir()
        else:
            raise ValueError()

        for file in csv_files:
            csv_names.append(file.name)
            file.replace(data_dir.joinpath(prefix).joinpath(file.name))

        for file in image_files:
            file.replace(data_dir.joinpath(prefix).joinpath(file.name))

        if len(csv_names) == 0:
            raise ValueError()
        return prefix
    except PermissionError:
        return False
    except ValueError:
        logging.error('Folder name is not sufficient')
        return False


def import_db(sep, tablename):
    prefix = move_files(tablename)

    if not prefix:
        raise ValueError()

    csv_files = data_dir.joinpath(prefix).rglob('*.csv')

    for file in csv_files:
        engine = create_engine('mysql://root:Nam123456@localhost/imagecrawler?charset=utf8mb4', echo=False)
        reader = pandas.read_csv(file, sep=sep, encoding='utf-8')
        try:
            reader.to_sql(tablename, engine, if_exists='append', index=False)
        except ValueError as err:
            logging.error(err)


@bp.route('/')
def index():
    db = get_db()

    cursor = db.cursor()
    tables = cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'imagecrawler';")
    tables = cursor.fetchall()

    return render_template('crawler/index.html', galleries=tables)


@bp.route('/detail')
def detail():
    tablename = request.args.get('id')

    if tablename is None:
        return redirect(url_for('crawler.index'))

    return render_template('crawler/detail.html', name=tablename)


quantity = 20


@bp.route("/load")
def load():
    tablename = request.args.get('id')

    db = get_db()
    res = None

    if request.args:
        counter = int(request.args.get("c"))  # The 'counter' value sent in the QS

        if counter == 0:
            print(f"Returning posts 0 to {quantity}")
            data = list()
            cursor = db.cursor()
            cursor.execute(f"SELECT * FROM {tablename} LIMIT {quantity}")
            rows = cursor.fetchall()
            for row in rows:
                data.append(list(row))
            res = make_response(jsonify(data), 200)

        else:
            print(f"Returning posts {counter} to {counter + quantity}")
            data = list()
            cursor = db.cursor()
            cursor.execute(f"SELECT * from {tablename} limit {quantity} offset {counter}")
            rows = cursor.fetchall()
            for row in rows:
                data.append(list(row))
            res = make_response(jsonify(data), 200)

    return res


@bp.route('/create', methods=('GET', 'POST'))
def create():
    if request.method == 'POST':
        tablename = request.form['tablename']
        sep = request.form['sep']
        site = request.form['site']
        url = request.form['url']
        error = None

        if not tablename:
            error = 'Table name is required.'

        if not sep:
            logging.info('using default sep')
            sep = ','

        if not site:
            error = 'Site is required.'

        if not url:
            error = 'URL is required.'

        if error is not None:
            flash(error)

        else:
            success = False
            if site == 'Kaggle':
                success = import_kaggle(url)
            elif site == 'GitHub':
                success = import_github(url)
            elif site == 'Box':
                success = import_box(url)

            if success:
                try:
                    import_db(sep, tablename)
                except ValueError:
                    flash('An error occurred')
                    return render_template('crawler/create.html')

                return redirect(url_for('crawler.index'))

    return render_template('crawler/create.html')
