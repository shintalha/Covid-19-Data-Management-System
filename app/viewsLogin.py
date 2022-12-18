from flask import Flask, render_template, flash, url_for, request, session
from werkzeug.utils import redirect
from model.user import *

def login_page():
    connection = User()
    session["id"] = None
    if(request.method == "POST"):
        check = request.args.get("check")
        if(check == "True"):
            mail = request.form["mailLogin"] if request.form["mailLogin"] !="" else None
            password = request.form["passwordLogin"]
            
            if mail is not None:
                information = connection.selectByEmailReturnPasswordAndID(mail)
                if(information is None):
                    flash("There is no such this user. Please register!")
                    return redirect("/")
                elif(password != information[0]):
                    flash("Wrong password!")
                    return redirect("/")
                else:
                    session["id"] = information[1]
                    return render_template("home.html", isHome=True)
            else:
                flash("Please enter a mail for log-in")
                return redirect("/")
        else:
            name = request.form["name"]
            surname = request.form["surname"]
            mail = request.form["mail"]
            password = request.form["password"]
            password_2 = request.form["password2"]
            adminPassword = request.form["adminPassword"]
            check_adminPassword = "Bu grup 100 alacak"
            try:
                isAdmin = "true" if request.form["isAdmin"] == 'on' else "false"
            except:
                isAdmin = "false"
            if((name !="")and(surname !="")and(mail !="")and(password !="")and(password_2 !="")):
                if password != password_2:
                    flash("These passwords are not same!")
                    return redirect("/")
                elif(isAdmin == "true" and adminPassword != check_adminPassword):
                    flash("Admin password is not correct! Nice try :)")
                    return redirect("/")
                else:
                    check=False
                    emailCheck = False
                    if connection.selectByEmailReturnPasswordAndID(email=mail) is not None:
                        emailCheck = True
                    else:
                        check = connection.register(name=name,surname=surname,email=mail,password=password,admin=isAdmin)
                    
                    if check:
                        flash("Successfully registered. Please log-in.")
                    else:
                        if emailCheck:
                            flash("This mail is used by another user")
                        else:
                            flash("Invalid entries!")
                    return render_template("login.html")
            else:
                flash("These fields cannot be blank!")
                return redirect("/")

    return render_template("login.html")        