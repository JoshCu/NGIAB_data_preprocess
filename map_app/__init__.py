from flask import Flask
from flask_cors import CORS
from .views import main, intra_module_db

app = Flask(__name__)
app.register_blueprint(main)

intra_module_db["app"] = app

CORS(app)