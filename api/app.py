from flask import Flask
import logging
logging.basicConfig(filename='error.log',
                    format='%(asctime)s.%(msecs)03d %(levelname)s - %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='a',
                    level=logging.ERROR
)


import config
cfg = config.settings_api

class MyFlask(Flask):
    jinja_options = dict(Flask.jinja_options)
    jinja_options.setdefault('extensions',
        []).append('jinja2_highlight.HighlightExtension')

app = MyFlask(__name__)

app.config['SECRET_KEY'] = cfg.SECRET_KEY
app.config['SQLALCHEMY_DATABASE_URI'] = cfg.SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = cfg.SQLALCHEMY_TRACK_MODIFICATIONS


if __name__ == "__main__":
   app.run(host='0.0.0.0', debug=True)
