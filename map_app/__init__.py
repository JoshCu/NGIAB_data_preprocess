from flask import Flask
from flask_cors import CORS


def create_app():
    app = Flask(__name__)

    from .views import main
    app.register_blueprint(main)
    
    return app

app = create_app()
CORS(app)