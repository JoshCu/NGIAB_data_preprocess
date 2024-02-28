from flask import Flask
from flask_cors import CORS

from .views import intra_module_db, main

app = Flask(__name__)
app.register_blueprint(main)

intra_module_db["app"] = app

CORS(app)