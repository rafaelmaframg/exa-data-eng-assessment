from math import ceil
from flask import Flask, render_template
from read_data import connect
from flask_paginate import Pagination, get_page_args

app = Flask('Patients_EMI')


mydb = connect()
cursor = mydb.cursor()
cursor.execute("SELECT unique_id, given_name, family_name, birth_date, social_security_number FROM patients")
myresult = cursor.fetchall()



def get_users(offset=0, per_page=10):
    return myresult[offset: offset + per_page]


@app.route('/')
def index():
    page, per_page, offset = get_page_args(page_parameter='page',
                                           per_page_parameter='per_page')
    total = len(myresult)
    pagination_users = get_users(offset=offset, per_page=per_page)
    pagination = Pagination(page=page, per_page=per_page, total=total,
                            css_framework='bootstrap4')
    return render_template('index.html',
                           users=pagination_users,
                           page=page,
                           per_page=per_page,
                           pagination=pagination,
                           )

@app.route("/patient/<id_user>")
def usuarios(id_user):
    return render_template("patient.html", id=id_user)


if __name__ == "__main__":
    app.run(debug=True)