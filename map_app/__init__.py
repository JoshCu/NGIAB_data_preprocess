from flask import Flask
from flask_cors import CORS
from .views import main

app = Flask(__name__)
app.register_blueprint(main)
CORS(app)