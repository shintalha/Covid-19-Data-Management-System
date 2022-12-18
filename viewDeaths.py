from flask import render_template, request, session
import numpy as np

from model.deaths import *
from model.user import *

def deaths_page(id = -1):
    user_id = str(session["id"])
    is_admin = False
    if user_id is not None:
        user = User()
        is_admin = user.isAdmin(user_id)

    page_id = request.args.get('page') if request.args.get('page') is not None else 1
    loc_name = request.args.get('loc_name') if request.args.get('loc_name') is not None else "?"
    date = request.args.get('date') if request.args.get('date') is not None else "?"
    table_size = 0

    page_id = int(page_id)

    deaths = Deaths()
    if id != -1:
        deaths.delete(int(id))
        
    loc_name = loc_name.replace("_"," ")
    
    loc_names = deaths.get_location_names()
    offset = (page_id-1)*50
    paginationValues = (page_id-1,page_id,page_id+1) if (page_id)>1 else (1,2,3)
    

    try:
        covid_data = np.array(deaths.read_filter(50,offset,loc_name,date))[:,0:9]
        table_size = covid_data.size
    except IndexError:
        covid_data = np.array([[]])
        table_size = 0
            
    start_dates = deaths.get_dates(loc_name)
    headers = ["Location Id", "Total Deaths", "New Deaths", "New Deaths Smoothed",
                "Total Deaths Per Million", "New Deaths Per Million", "New Deaths Smoothed Per Million","Date "]

    return render_template("deaths/deaths.html", table_headers=headers, table_rows = covid_data, \
        paginationValues=paginationValues, locations = loc_names, dates = start_dates, data_available=table_size, is_admin=is_admin) 

def add_deaths_page():
    covid_test = Deaths()
    message = "empty"

    if request.method == "POST":   
        location_id = request.form["location_id"]
        total_deaths = request.form["total_deaths"]
        new_deaths = request.form["new_deaths"] 
        total_deaths_per_million = request.form["total_deaths_per_million"] if request.form["total_deaths_per_million"] !="" else None
        new_deaths_per_million= request.form["new_deaths_per_million"] if request.form["new_deaths_per_million"] !="" else None
        new_deaths_smoothed = request.form["new_deaths_smoothed"] if request.form["new_deaths_smoothed"] !="" else None
        new_deaths_smoothed_per_million = request.form["new_deaths_smoothed_per_million"] if request.form["new_deaths_smoothed_per_million"] !="" else None
        date_time = request.form["date_time"] if request.form["date_time"] !="" else None
        result = covid_test.insert_row(location_id, total_deaths, new_deaths, total_deaths_per_million, new_deaths_per_million, new_deaths_smoothed, new_deaths_smoothed_per_million, date_time)
        if result:
            message = "success"  
        else:
            message = "failed" 
    return render_template("deaths/add-deaths.html", message=message) 

def update_deaths_page():
    row_id = request.args.get('id')
    
    row_id = int(row_id)

    deaths = Deaths()
    row = np.array(deaths.readFid(row_id))
    message = "empty"
    if request.method == "POST":
        total_deaths = request.form["total_deaths"] if request.form["total_deaths"] !="" else row[2]
        new_deaths = request.form["new_deaths"] if request.form["new_deaths"] !="" else row[3]
        total_deaths_per_million = request.form["total_deaths_per_million"] if request.form["total_deaths_per_million"] !="" else row[4]
        new_deaths_per_million = request.form["new_deaths_per_million"] if request.form["new_deaths_per_million"] !="" else row[5]
        new_deaths_smoothed = request.form["new_deaths_smoothed"] if request.form["new_deaths_smoothed"] !="" else row[6]
        new_deaths_smoothed_per_million = request.form["new_deaths_smoothed_per_million"] if request.form["new_deaths_smoothed_per_million"] !="" else row[7]
        date_time = request.form["date_time"] if request.form["date_time"] !="" else row[8]
        result = deaths.update_row(row_id, row[1], total_deaths, new_deaths, total_deaths_per_million, new_deaths_per_million, new_deaths_smoothed, new_deaths_smoothed_per_million, date_time)
        if result:
            message = "success"  
        else:
            message = "failed" 
    
    return render_template("deaths/update-deaths.html", id = row_id, data=row, message=message)