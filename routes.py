from flask import Flask, render_template
from generate_data import connect, read_event, read_user, read_number,create_db,create_table, add_data
from flask_paginate import Pagination, get_page_args
import datetime

app = Flask('Patients_EMI')

###################################################
#     Connection to get the user data to index    #
###################################################
MYSQL_OFF = False
try:
    mydb = connect()
    cursor = mydb.cursor()
    cursor.execute("""SELECT unique_id, given_name, family_name,
             birth_date, social_security_number FROM patients""")
    myresult = cursor.fetchall()
    cursor.close()
    mydb.close()
except:
    try:
        if not connect():
            create_db()
            conn = connect()
        else:
            conn = connect()
        cursor = conn.cursor()
        create_table(conn)
        add_data(cursor, conn)

    except:
        MYSQL_OFF = True

#######################################################################################

@app.route("/search/<word>")
def search(word):
    """
    Route to search page, this is created to facilitate access to patients
    :parameters: word (str) website search input field
    """
    results = []
    for each in myresult:
        try:
            for data in each:
                if isinstance(data, datetime.date):
                    data = data.strftime("%Y-%m-%d")
                if word.lower() in data.lower():
                    results.append(each)
        except:
            continue
    page, per_page, offset = get_page_args(page_parameter='page',
                                           per_page_parameter='per_page')
    total = len(results)
    pagination_users2 = results[offset: offset + per_page]
    pagination = Pagination(page=page, per_page=per_page, total=total,
                            css_framework='bootstrap4')
   
    return render_template("index.html", users=pagination_users2,
                           page=page,
                           per_page=per_page,
                           pagination=pagination)


@app.route('/')
def index():
    """
    Route for index, check if the Mysql connection is ON and render a error page if not
    """
    if MYSQL_OFF == True:
        return "ERROR With Sql_DB"
    page, per_page, offset = get_page_args(page_parameter='page',
                                           per_page_parameter='per_page')
    total = len(myresult)
    pagination_users =  myresult[offset: offset + per_page]
    pagination = Pagination(page=page, per_page=per_page, total=total,
                            css_framework='bootstrap4')
    return render_template('index.html',
                           users=pagination_users,
                           page=page,
                           per_page=per_page,
                           pagination=pagination,
                           )

@app.route("/detail/<id_user>")
def details(id_user):
    """
    Route to the registers about the patient (Claims, etc)
    :parameters: id_user (str) unique id from the patient db
    """
    result = read_user(id_user, "patient_event")
    return str(result)

@app.route("/patient/<id_user>")
def usuarios(id_user):
    """
    Route to render the details about the patient and check how many registers are(Claims, etc)
    :parameters: id_user (str) unique id from the patient db
    """
    result = read_user(id_user, 'patients')
    counter = read_number(id_user)
    if counter == (0,):
        read_event(id_user)
        counter = read_number(id_user)
    return render_template("patient.html", user=result[0], counter=counter)


if __name__ == "__main__":
    app.run(debug=True)