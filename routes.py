from math import ceil
from flask import Flask, render_template
from generate_data import connect, read_id, read_user, read_number
from flask_paginate import Pagination, get_page_args

app = Flask('Patients_EMI')


###################################################
#        Connection to get the user data          #
###################################################
mydb = connect()
cursor = mydb.cursor()
cursor.execute("SELECT unique_id, given_name, family_name, birth_date, social_security_number FROM patients")
myresult = cursor.fetchall()
cursor.close()
mydb.close()
######################################################

@app.route("/search/<word>")
def search(word):
    results = []
    for each in myresult:
        try:
            if each.index(word):
                results.append(each)
        except:
            continue
    print(results)
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
    result = read_user(id_user, "patient_event")
    return str(result)

@app.route("/patient/<id_user>")
def usuarios(id_user):
    #TODO documentation about this request
    result = read_user(id_user, 'patients')
    counter = read_number(id_user)
    if counter == (0,):
        read_id(id_user)
        counter = read_number(id_user)
    return render_template("patient.html", user=result[0], counter=counter)


if __name__ == "__main__":
    app.run(debug=True)