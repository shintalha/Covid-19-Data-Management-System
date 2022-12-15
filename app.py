from datetime import datetime

from flask import Flask, render_template

from views.views import *


app = Flask(__name__)
app.secret_key = "abc"  

@app.route("/")
def login_page():
    return render_template("login.html")


app.add_url_rule("/locations", view_func=locations_page)
app.add_url_rule("/locations/<loc_name>", view_func=location_page)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)