# Covid 19 Data Management System Project by using Flask and PostgreSQL

The app is a website where users can view information about Covid in different countries and
dates. Also, data can be added or changed by admins. In order to use the website users need
to sign up first as a normal user or admin.

It was developed with me and my 4 friends in my team.
We used HTML and CSS and Bootstrap for the front-end. In the back-end, we used Flask packages for database connections.

Setup Folder:
The files called SetupName.py are the files that create the table under the responsibility of that person in the database and extract the relevant data from the dataset.csv file and insert it into this table. Locations and User Table are the joint responsibility of the team.

Model folder:
It contains files where each table has its own functions (such as get, insert, update, delete).

Static folder:
It hosts the bootstrap files and the css files we created.

Templates:
It is the folder where the html forms of the related tables editing/viewing screens are located.

You can read our project report from [here](https://github.com/shintalha/Database-Project-With-Flask/blob/main/Project%20Report.pdf)

### Entity Relation Diagram
![ER  Diagram](https://user-images.githubusercontent.com/97956471/215353777-94df8ee8-35be-48bd-85d8-0dd4ccf3945a.jpg)
