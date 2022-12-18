from flask import render_template, flash, request, session
from werkzeug.utils import redirect
import numpy as np
from datetime import datetime

from model.locations import *
from model.user import *
from model.hospital_and_icu import *

def patients_page():
    connection = hospital_and_icu()
    #pagination
    countryName = request.args.get("countryName")
    dateFilter = request.args.get("dateVariable")
    pageNumber =request.args.get("pageNumber") if request.args.get("pageNumber") is not None else "1"
    pageNumber = int(pageNumber)
    offset = (pageNumber-1)*50
    paginationValues = (pageNumber,pageNumber+1,pageNumber+2) if (pageNumber)>0 else (0,1,2)

    location = Locations()
    countries = connection.get_country_names()
    patients = None
    isadmin = False
    user_id = str(session["id"])
    if user_id is not None:
        user = User()
        isadmin = user.isAdmin(user_id)
    headings = ("icu_patients", "icu_patients_per_million","hosp_patients" ,"hosp_patients_per_million" , "weekly_icu_admissions" ,"weekly_icu_admissions_per_million" ,"weekly_hosp_admissions" ,"weekly_hosp_admissions_per_million")
    if(countryName is not None and dateFilter ==''):
        country_id = location.get_id_by_country_name(countryName)
        result = connection.selectFromLOC(country_id[0], offset)
    elif(countryName is not None and dateFilter is not None):
        country_id = location.get_id_by_country_name(countryName)
        result = connection.selectFromLOCandDate(country_id[0], dateFilter)
    else:
        result = connection.selectAll(offset)
    patients = np.zeros([1, 10], dtype='str')
    if(result is not None):
        for row in result:
            newRow = np.array(row)
            patients = np.vstack([patients, newRow])

    patients = np.delete(patients, 0, 0)
    return render_template("patients/patients.html",headings=headings, isadmin=isadmin,  patients=patients, countries=countries, paginationValues=paginationValues)

def add_patients_data():
    connection = hospital_and_icu()
    loc_con = Locations()
    countries = loc_con.get_country_names()
    headings = ("icu_patients", "icu_patients_per_million","hosp_patients" ,"hosp_patients_per_million" , "weekly_icu_admissions" ,"weekly_icu_admissions_per_million" ,"weekly_hosp_admissions" ,"weekly_hosp_admissions_per_million")

    if request.method == "POST":
        if(request.form["country"] and request.form["date"]):
            location_id = request.form["country"]
            location_id = loc_con.get_id_by_country_name(location_id)
            date_time = request.form["date"]
        else:
            flash("Both country and date fields cannot be blank")
            return render_template("patients/add.html", headings = headings, countries = countries)
        if(request.form["icu_patients"] or request.form["hosp_patients"]):
            icu_patients = request.form["icu_patients"] if request.form["icu_patients"] !="" else "NULL"
            hosp_patients = request.form["hosp_patients"] if request.form["hosp_patients"] !="" else "NULL"
        else:
            flash("Either icu_patients or hospital_patients cannot be blank!")
            return render_template("patients/add.html", headings = headings, countries = countries)
        icu_patients_per_million = request.form["icu_patients_per_million"] if request.form["icu_patients_per_million"] !="" else "NULL"
        hosp_patients_per_million = request.form["hosp_patients_per_million"] if request.form["hosp_patients_per_million"] !="" else "NULL"
        weekly_hosp_admissions = request.form["weekly_hosp_admissions"] if request.form["weekly_hosp_admissions"] !="" else "NULL"
        weekly_hosp_admissions_per_million = request.form["weekly_hosp_admissions_per_million"] if request.form["weekly_hosp_admissions_per_million"] !="" else "NULL"
        weekly_icu_admissions = request.form["weekly_icu_admissions"] if request.form["weekly_icu_admissions"] !="" else "NULL"
        weekly_icu_admissions_per_million = request.form["weekly_icu_admissions_per_million"] if request.form["weekly_icu_admissions_per_million"] !="" else "NULL"
        
        country_id_fetched  = loc_con.is_there(location_id[0])

        if country_id_fetched is None:
            flash("Please enter a valid country")
            return render_template("patients/add.html", headings=headings, countries=countries)
        
        (country_id,) = country_id_fetched

        format = "%Y-%m-%d"

        try:
            datetime.strptime(date_time, format)
        except ValueError:
            flash("Please enter a valid date in the format YYYY-MM-DD")
            return render_template("patients/add.html", headings=headings,countries=countries)

        check_q = connection.is_there(date_time, country_id)
        if check_q:
            flash("You can not add a new record into an already existing record")
            return render_template("patients/add.html",headings=headings,  countries=countries)
        
        connection.insert(country_id, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, date_time)
        
        flash("Successfully created")
        return render_template("patients/add.html", headings=headings, countries=countries)
    else:
        return render_template("patients/add.html", headings=headings, countries=countries)
    
def update_patients_data():
    connection = hospital_and_icu()
    loc_con = Locations()
    countries = loc_con.get_country_names()
    headings = ("icu_patients", "icu_patients_per_million","hosp_patients" ,"hosp_patients_per_million" , "weekly_icu_admissions" ,"weekly_icu_admissions_per_million" ,"weekly_hosp_admissions" ,"weekly_hosp_admissions_per_million")

    if request.method == "POST":
        if(request.form["country"] and request.form["date"]):
            location_id = request.form["country"]
            location_id = loc_con.get_id_by_country_name(location_id)
            date_time = request.form["date"]
        else:
            flash("Both country and date fields cannot be blank")
            return render_template("patients/update.html", headings = headings, countries = countries)
        if(request.form["icu_patients"] or request.form["hosp_patients"]):
            icu_patients = request.form["icu_patients"] if request.form["icu_patients"] !="" else "NULL"
            hosp_patients = request.form["hosp_patients"] if request.form["hosp_patients"] !="" else "NULL"
        else:
            flash("Either icu_patients or hospital_patients cannot be blank!")
            return render_template("patients/update.html", headings = headings, countries = countries)
        icu_patients_per_million = request.form["icu_patients_per_million"] if request.form["icu_patients_per_million"] !="" else "NULL"
        hosp_patients_per_million = request.form["hosp_patients_per_million"] if request.form["hosp_patients_per_million"] !="" else "NULL"
        weekly_hosp_admissions = request.form["weekly_hosp_admissions"] if request.form["weekly_hosp_admissions"] !="" else "NULL"
        weekly_hosp_admissions_per_million = request.form["weekly_hosp_admissions_per_million"] if request.form["weekly_hosp_admissions_per_million"] !="" else "NULL"
        weekly_icu_admissions = request.form["weekly_icu_admissions"] if request.form["weekly_icu_admissions"] !="" else "NULL"
        weekly_icu_admissions_per_million = request.form["weekly_icu_admissions_per_million"] if request.form["weekly_icu_admissions_per_million"] !="" else "NULL"
        
        country_id_fetched  = loc_con.is_there(location_id[0])

        if country_id_fetched is None:
            flash("Please enter a valid country")
            return render_template("patients/update.html", headings=headings, countries=countries)
        
        (country_id,) = country_id_fetched

        format = "%Y-%m-%d"

        try:
            datetime.strptime(date_time, format)
        except ValueError:
            flash("Please enter a valid date in the format YYYY-MM-DD")
            return render_template("patients/update.html", headings=headings,countries=countries)

        check_q = connection.is_there(date_time, country_id)
        if check_q:
            flash("You can not update non-exist record")
            return render_template("patients/update.html",headings=headings,  countries=countries)
        
        connection.update(country_id, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, date_time)
        
        flash("Successfully updated")
        return render_template("patients/update.html", headings=headings, countries=countries)
    else:
        return render_template("patients/update.html", headings=headings, countries=countries)

def edit_patients_page():
    connection = hospital_and_icu()
    #pagination
    countryName = request.args.get("countryName")
    dateFilter = request.args.get("dateVariable")
    pageNumber =request.args.get("pageNumber") if request.args.get("pageNumber") is not None else "1"
    pageNumber = int(pageNumber)
    offset = (pageNumber-1)*50
    paginationValues = (pageNumber,pageNumber+1,pageNumber+2) if (pageNumber)>0 else (0,1,2)
    if(request.args.get("deleteMode") == "on"):
        if(countryName != '' and dateFilter != ''):
            delete_patients(countryName, dateFilter)
        else:
            flash("For delete operation, Date or Country field cannot be blank!")
        return redirect("/patients/edit")
    else:
        location = Locations()
        countries = connection.get_country_names()
        patients = None
        headings = ("icu_patients", "icu_patients_per_million","hosp_patients" ,"hosp_patients_per_million" , "weekly_icu_admissions" ,"weekly_icu_admissions_per_million" ,"weekly_hosp_admissions" ,"weekly_hosp_admissions_per_million")
        if(countryName is not None and dateFilter ==''):
            country_id = location.get_id_by_country_name(countryName)
            result = connection.selectFromLOC(country_id[0], offset)
        elif(countryName is not None and dateFilter is not None):
            country_id = location.get_id_by_country_name(countryName)
            result = connection.selectFromLOCandDate(country_id[0], dateFilter)
        else:
            result = connection.selectAll(offset)
        patients = np.zeros([1, 10], dtype='str')
        if(result is not None):
            for row in result:
                newRow = np.array(row)
                patients = np.vstack([patients, newRow])

        patients = np.delete(patients, 0, 0)
        return render_template("patients/edit-patients.html",headings=headings,  patients=patients, countries=countries, paginationValues=paginationValues)

def delete_patients(country_id,date):
    connection = hospital_and_icu()
    loc_con = Locations()
    check = connection.selectFromLOCandDateReturnID(country_id, date)
    if(check is not None):
        connection.delete(check)
    else:
        flash("There is no this record in database")
        return redirect("/patients/edit")


