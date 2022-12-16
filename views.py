from datetime import datetime

from flask import Flask, render_template, flash, url_for
from werkzeug.utils import redirect

import numpy as np

import os

from model.locations import *
from viewHospitalAndIcu import *
from viewCovidTests import *
from viewDeaths import *
from viewVaccinations import *
from viewCases import *
from viewsLogin import *

def locations_page():
    chart_paths = os.path.join('static', 'charts')
    locations_table = Locations()
    loc_count = np.array(locations_table.get_country_names()).shape[0]
    loc_list = np.array(locations_table.get_country_names()).reshape(-1,loc_count)[0]
    return render_template("locations/locations.html", locations = loc_list, charts=[chart_paths+i for i in ["\\aged_65_older.png",\
                                                                        "\\aged_70_older.png", "\\location_pop.png", "\\median_age.png"]])

def location_page(loc_name):
    locations_table = Locations()
    loc_id = locations_table.get_id_by_country_name(loc_name)
    if loc_id == None:
        return render_template("locations/location.html", location_info = {"location":-1})
    loc_info = locations_table.find_by_id(loc_id[0])
    print(dict(zip(locations_table.columns,loc_info)))
    return render_template("locations/location.html", location_info = dict(zip(locations_table.columns,loc_info)))

def home_page():
    return render_template("home.html")
