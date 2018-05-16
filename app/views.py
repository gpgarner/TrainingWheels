from flask import jsonify

from flask import render_template, request

from flask import Flask

from flask_sqlalchemy import SQLAlchemy

from app import app


# the values of those depend on your setup
POSTGRES_URL = 'postgresgpgsql.c0npzf7zoofq.us-east-1.rds.amazonaws.com:5432'
POSTGRES_USER = 'gpgarner8324'
POSTGRES_PW ='carmenniove84!'
POSTGRES_DB = 'postgresMVP'

DB_URL = 'postgresql+psycopg2://{user}:{pw}@{url}/{db}'.format(user=POSTGRES_USER,pw=POSTGRES_PW,url=POSTGRES_URL,db=POSTGRES_DB)

app.config['SQLALCHEMY_DATABASE_URI'] = DB_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # silence the deprecation warning

db = SQLAlchemy(app)
@app.route('/')


@app.route('/index')
def index():
	db = SQLAlchemy()

	POSTGRES = {
		'user': ,
		'pw': ,
		'db': ,
		'host': 'localhost',
		'port': '5432'
	}

	app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://%(user)s:\%(pw)s@%(host)s:%(port)s/%(db)s' % POSTGRES

