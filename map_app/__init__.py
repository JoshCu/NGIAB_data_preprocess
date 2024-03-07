from flask import Flask
from flask_cors import CORS

import logging
from .views import intra_module_db, main

logging.getLogger("werkzeug").setLevel(logging.WARNING)

app = Flask(__name__)
app.register_blueprint(main)

intra_module_db["app"] = app

CORS(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(name)-12s: %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",
)  # Append mode
# Example: Adding a console handler to root logger (optional)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Or any other level
formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger("").addHandler(console_handler)
