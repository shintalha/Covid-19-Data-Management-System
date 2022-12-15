from datetime import datetime

from flask import Flask, render_template

from views import *


app = Flask(__name__)
app.secret_key = "abc"  

@app.route("/")
def login_page():
    return render_template("login.html")

app.add_url_rule("/locations", view_func=locations_page)
app.add_url_rule("/locations/<loc_name>", view_func=location_page)
app.add_url_rule("/patients", view_func=patients_page, methods=["GET", "POST"])
app.add_url_rule("/patients/add", view_func=add_patients_data, methods=["GET", "POST"])
app.add_url_rule("/patients/update", view_func=update_patients_data, methods=["GET", "POST"])
app.add_url_rule("/patients/edit", view_func=edit_patients_page, methods=["GET", "POST"])

app.add_url_rule("/home", view_func=home_page, methods=["GET", "POST"])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)