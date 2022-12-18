from flask import render_template, request, session, redirect
import numpy as np
from model.locations import Locations
from model.cases import cases
from model.user import *

# cases
def cases_page(id = -1):
    user_id = str(session["id"])
    isAdmin = False
    if user_id is not None and user_id != "None":
         user = User()
         isadmin = user.isAdmin(user_id)
    else:
        return redirect("/")
    
    if id != -1 and isAdmin:
        cases.delete(int(id))

    locations = Locations()    
    pageNumber = int(request.args.get('page')) if request.args.get('page') is not None else 1
    countryName = request.args.get('loc_name') if request.args.get('loc_name') is not None else "?"

    pageNumber = int(pageNumber)
    offset = (pageNumber-1)*100

    countries = None
    casesData = None
    headings = ["Location Id", "Total Cases", "New Cases", "Total Cases PM",
                "New Cases PM", "New Cases SPM", "Date"]

    countries = locations.get_country_names()
    countriesData = []
    for row in countries:
        countriesData.append(row[0])
    

    location_id = locations.get_id_by_country_name(country=countryName)

    result = None
    if countryName != '?':
        result = cases.Get100ByOffsetAndCountry(country=location_id, offset=offset)
    else:
        result = cases.Get100ByOffset(offset=offset)

    casesData = np.zeros([1, 8], dtype='str')
    for row in result:
        newRow = np.array(row)
        casesData = np.vstack([casesData, newRow])

    casesData = np.delete(casesData, 0, 0)
    return render_template("cases/cases.html", table_headers=headings, locations=countriesData, table_rows=casesData, isAdmin=isAdmin)

def update_cases_page(id = -1):
    user_id = str(session["id"])
    isAdmin = False
    user_id = str(session["id"])
    isAdmin = False
    if user_id is not None and user_id != "None":
         user = User()
         isadmin = user.isAdmin(user_id)
    else:
        return redirect("/")

    if isAdmin is False:
        return redirect("/")
    
    message = "empty"
    updateData = None

    if id != -1:
        updateData = cases.findById(id=id)

    if request.method == "POST":   
        cases_id = request.form["cases_id"]
        location_id = request.form["location_id"]
        total_cases = request.form["total_cases"]
        new_cases = request.form["new_cases"]
        total_cases_per_million = request.form["total_cases_per_million"] if request.form["total_cases_per_million"] !="" else "NULL"
        new_cases_per_million = request.form["new_cases_per_million"] if request.form["new_cases_per_million"] !="" else "NULL"
        new_cases_smoothed_per_million = request.form["new_cases_smoothed_per_million"] if request.form["new_cases_smoothed_per_million"] !="" else "NULL"
        date_time = request.form["date_time"]
        result = cases.update(cases_id, location_id, total_cases, new_cases, total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million, date_time)
        if result:
            message = "success"  
        else:
            message = "failed" 
    
    return render_template("cases/update-cases.html", data=updateData, message=message)

def add_cases_page():
    user_id = str(session["id"])
    isAdmin = False
    user_id = str(session["id"])
    isAdmin = False
    if user_id is not None and user_id != "None":
         user = User()
         isadmin = user.isAdmin(user_id)
    else:
        return redirect("/")

    if isAdmin is False:
        return redirect("/")
    
    message = "empty"

    if request.method == "POST":   
        location_id = request.form["location_id"]
        total_cases = request.form["total_cases"]
        new_cases = request.form["new_cases"]
        total_cases_per_million = request.form["total_cases_per_million"] if request.form["total_cases_per_million"] !="" else "NULL"
        new_cases_per_million = request.form["new_cases_per_million"] if request.form["new_cases_per_million"] !="" else "NULL"
        new_cases_smoothed_per_million = request.form["new_cases_smoothed_per_million"] if request.form["new_cases_smoothed_per_million"] !="" else "NULL"
        date_time = request.form["date_time"]
        result = cases.save(location_id, total_cases, new_cases, total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million, date_time)
        if result:
            message = "success"  
        else:
            message = "failed" 
    
    return render_template("cases/add-cases.html", message=message)
        


            
