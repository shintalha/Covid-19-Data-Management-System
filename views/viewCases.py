from flask import render_template, request
import numpy as np
from model.locations import Locations
from model.cases import cases


# cases
def cases_page():
    #pagination
    countryName = request.args.get("countryName")
    pageNumber =request.args.get("pageNumber") if request.args.get("pageNumber") is not None else "1"
    pageNumber = int(pageNumber)
    offset = (pageNumber-1)*50
    paginationValues = (pageNumber,pageNumber+1,pageNumber+2) if (pageNumber)>0 else (0,1,2)

    countries = None
    casesData = None
    headings = ("location_id","total_cases", "new_cases", "total_cases_per_million", "new_cases_per_million", "new_cases_smoothed_per_million", "date_time")
    
        
    countries = Locations.get_country_names()
    location_id = Locations.get_id_by_country_name(country=countryName)
    

    if countryName is not None:
        result = cases.findByLocationId(location_id=location_id)
    else: 
        result = cases.findAll()
    
    casesData = np.zeros([1, 8], dtype='str')
    for row in result:
        newRow = np.array(row)
        casesData = np.vstack([casesData, newRow])

    casesData = np.delete(cases, 0, 0)
    return render_template("cases/cases.html", paginationValues=paginationValues, headings=headings, cases=casesData, countries=countries)


