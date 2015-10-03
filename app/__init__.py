from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.from_object('config')

# this might change the way i use sqlalchemy with in the application.
#	but why use the extention if i'm efficient at using sqlalchemy.
# if you use the flask.ext.sqlalchemy extension... requires changes in the models
db = SQLAlchemy(app)

# VS USING:
class CustomAlchemy(SQLAlchemy):
    def make_declarative_base(self):
        base = declarative_base(...)
        ...
        return base
# interesting post on stack, about overwriting the extensions make_declarative_base() with sqlAlchemy's declarative_base() will need to test this???
db = CustomAlchemy()


from app import views, models



