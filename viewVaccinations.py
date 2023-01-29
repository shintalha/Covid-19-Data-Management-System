from flask import render_template, request, session, redirect
import numpy as np

from model.vaccinations import *
from model.user import *

def vaccinations_page(id = -1):
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

    vaccinations = Vaccinations()
    if id != -1:
        vaccinations.delete(int(id))
        
    loc_name = loc_name.replace("_"," ")
    
    loc_names = vaccinations.get_location_names()
    offset = (page_id-1)*50
    paginationValues = (page_id-1,page_id,page_id+1) if (page_id)>1 else (1,2,3)
    

    try:
        covid_data = np.array(vaccinations.read_filter(50,offset,loc_name,date))[:,0:9]
        table_size = covid_data.size
    except IndexError:
        covid_data = np.array([[]])
        table_size = 0
            
    start_dates = vaccinations.get_dates(loc_name)
    headers = [" ".join(head.split("_")).title() for head in vaccinations.columns]

    return render_template("vaccinations/vaccinations.html", table_headers=headers, table_rows = covid_data, \
        paginationValues=paginationValues, locations = loc_names, dates = start_dates, data_available=table_size, is_admin=is_admin) 

def add_vaccinations_page():
    vaccinations = Vaccinations()
    message = "empty"

    if request.method == "POST":   
        location_id = request.form["location_id"]
        total_vaccinations = request.form["total_vaccinations"]
        people_vaccinated = request.form["people_vaccinated"] 
        people_fully_vaccinated = request.form["people_fully_vaccinated"] if request.form["people_fully_vaccinated"] !="" else None
        total_boosters = request.form["total_boosters"] if request.form["total_boosters"] !="" else None
        new_vaccinations = request.form["new_vaccinations"] if request.form["new_vaccinations"] !="" else None
        new_vaccinations_smoothed = request.form["new_vaccinations_smoothed"] if request.form["new_vaccinations_smoothed"] !="" else None
        date_time = request.form["date_time"] if request.form["date_time"] !="" else None
        result = vaccinations.insert_row(location_id, total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters, new_vaccinations, new_vaccinations_smoothed, date_time)
        if result:
            message = "success"  
        else:
            message = "failed" 
    return render_template("vaccinations/add-vaccinations.html", message=message) 

def update_vaccinations_page():

    row_id = request.args.get('id')
    row_id = int(row_id)
    vaccinations = Vaccinations()
    row = np.array(vaccinations.read_with_id(row_id))
    message = "empty"
    if request.method == "POST":
        total_vaccinations = request.form["total_vaccinations"] if request.form["total_vaccinations"] !="" else row[2]
        people_vaccinated = request.form["people_vaccinated"] if request.form["people_vaccinated"] !="" else row[3]
        people_fully_vaccinated = request.form["people_fully_vaccinated"] if request.form["people_fully_vaccinated"] !="" else row[4]
        total_boosters = request.form["total_boosters"] if request.form["total_boosters"] !="" else row[5]
        new_vaccinations = request.form["new_vaccinations"] if request.form["new_vaccinations"] !="" else row[6]
        new_vaccinations_smoothed = request.form["new_vaccinations_smoothed"] if request.form["new_vaccinations_smoothed"] !="" else row[7]
        date_time = request.form["date_time"] if request.form["date_time"] !="" else row[8]
        result = vaccinations.update(row_id, row[1], total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters, new_vaccinations, new_vaccinations_smoothed, date_time)
        if result:
            message = "success"  
        else:
            message = "failed" 
    
    return render_template("vaccinations/update-vaccinations.html", id = row_id, data=row, message=message)