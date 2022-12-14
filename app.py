from flask import Flask
from views import *

app = Flask(__name__)

# case table pages
app.add_url_rule("/cases", view_func=cases_page, methods=["GET", "POST"])
