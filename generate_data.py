from ast import expr
import os
import json
import mysql.connector
import pandas as pd


def connect():
    """
    Funtion to connect using mysql connector and verify if the db exists
    :return: conn (mysql.connector Object) or False if unable to connect
    """
    try:
        conn = mysql.connector.connect(
        host="mysql-container",
        user="root",
        password="testflask",
        database="patients_db",
        )
        return conn
    except:
        return False

def create_db():
    """
    Function to create a DB 
    """
    try:
        conn = mysql.connector.connect(
        host="mysql-container",
        user="root",
        password="testflask",
        )
        print('ok conectado')
        mycursor = conn.cursor()
        mycursor.execute("CREATE DATABASE patients_db")
        mycursor.close()
        conn.close()
    except:
        raise RuntimeError("Connection ERROR!! Check the mysql server")


def create_table(conn):
    """
    Function to create the tables if the tables don't exist in the DB
    :parameters: mydb (Mysql connection object) 
    """
    mycursor = conn.cursor()
    mycursor.execute("SHOW TABLES")
    myresult = mycursor.fetchall() 
    if myresult == []:
        print("Creating DB...")
        mycursor.execute("CREATE TABLE patients(unique_ID VARCHAR(100) NOT NULL PRIMARY KEY, given_name VARCHAR(20), family_name VARCHAR(50), birth_date DATE, us_core_race VARCHAR(30), us_core_ethnicity VARCHAR(50), mothers_maiden_name VARCHAR(100), us_core_birthsex CHAR, gender VARCHAR(20), birth_city VARCHAR(50), birth_state VARCHAR(50), birth_country CHAR(2), disability_adjusted_life_years FLOAT(15), quality_adjusted_life_years FLOAT(15), medical_record_number VARCHAR(50), social_security_number VARCHAR(50), driver_license VARCHAR(50), passport_number VARCHAR(50), phone_number VARCHAR(20), address_line VARCHAR(50), address_city VARCHAR(50), address_country VARCHAR(2), marital_status CHAR, multiple_birth VARCHAR(255), language VARCHAR(10) )")
        conn.commit()
        mycursor = conn.cursor()
        mycursor.execute("CREATE TABLE patient_event (patient_event_id MEDIUMINT NOT NULL AUTO_INCREMENT, unique_ID VARCHAR(100), "
        "event_data LONGTEXT, type VARCHAR(255), PRIMARY KEY(patient_event_id), FOREIGN KEY (unique_ID) REFERENCES "
        "patients(unique_ID))")
    mycursor.close()

def add_event(event, unique_id, cursor):
    """
    Function that receive the event and unique ID and insert to table events in DB 
    :parameter: event (dict) data different than "Patient", that mean it"s a claim, or other data to save
    :parameter: unique_id (str) patient unique id
    :parameter: cursor (Mysql Obect) 
    """
    resource_event = json.dumps(event["resource"]).replace('"', '""').replace("'", "''")
    cursor.execute(f"""INSERT INTO `patients_db`.`patient_event`(`patient_event_id`,`unique_ID`,`event_data`,  `type`)VALUES (NULL, '{unique_id}', '{resource_event}', '{event["resource"]["resourceType"]}')""")

def read_number(unique_id):
    """
    Function to read the number of registers about the patient to show in the details page
    :parameters: unique_id (str) patient unique id
    :return: result[0] (list) return a list at the index 0 with all the details about the patient 
    """
    mydb = connect()
    cursor = mydb.cursor()
    cursor.execute(f"SELECT count(unique_ID) FROM patient_event WHERE unique_ID = '{unique_id}' ")
    result = cursor.fetchall()
    return result[0]

def read_user(unique_id, table):
    """
    Function to read the user details from the table selected
    :parameter: unique_id (Str) unique Id about each patient
    :parameter: table (Str) name of the table to select the elements
    :return: result (list) list about the query
    """
    mydb = connect()
    cursor = mydb.cursor()
    cursor.execute(f"SELECT * FROM {table} WHERE unique_ID = '{unique_id}'")
    result = cursor.fetchall()
    if result == []:
        read_event(unique_id)
        mydb = connect()
        cursor = mydb.cursor()
        cursor.execute(f"SELECT * FROM {table} WHERE unique_id = '{unique_id}'")
        result = cursor.fetchall()
    return result

############################################################################################################
# function created to reduce program loading time by adding information according to what is being queried #
############################################################################################################
def read_event(unique_id):
    """
    Function to read the JSON with the unique_id and check each event
    :parameters: unique_id (str) patient unique id
    """
    conn = connect()
    mycursor = conn.cursor()
    for item in os.listdir("data"):
        if unique_id in item:
            item = os.path.join("data", item)
            with open(item, encoding="utf-8") as f:
                patient = json.load(f)
                unique_id = patient["entry"][0]["resource"]["id"]
                for entry in patient["entry"]:
                    if entry["resource"]["resourceType"] != "Patient":
                        add_event(entry, unique_id, mycursor)
    conn.commit()

def add_data(mycursor, mydb):
    """
    Function to read and add the JSON data into DB 
    :parameter: mycursor (Mysql Cursor Object) - Cursor to execute the query
    :parameter: mydb (Mysql Connection Object) - Connection with DB 
    """
    for item in os.listdir("data"):
        item = os.path.join("data", item)
        with open(item, encoding="utf-8") as f:
            patient = json.load(f)
            patient = patient["entry"][0]["resource"]
            #exception to driver license
            try:
                driver = patient["identifier"][3]["value"]
            except IndexError:
                driver = "N/A"
            #exception to passaport
            try:
                passport = patient["identifier"][5]["value"]
            except IndexError:
                passport = "N/A"
            try:
                mycursor.execute(f"""INSERT INTO patients(`unique_ID`, `given_name`, `family_name`,
                `birth_date`, `us_core_race`, `us_core_ethnicity`, `mothers_maiden_name`, `us_core_birthsex`, `gender`, `birth_city`,`birth_state`, `birth_country`, `disability_adjusted_life_years`, `quality_adjusted_life_years`, `medical_record_number`,`social_security_number`, `driver_license`, `passport_number`, `phone_number`, `address_line`, `address_city`, `address_country`, `marital_status`, `language`)
                 VALUES("{patient["id"]}", "{patient["name"][0]["given"][0]}", "{patient["name"][0]["family"]}",
                "{patient["birthDate"]}", "{patient["extension"][0]["extension"][1]["valueString"]}",
                "{patient["extension"][1]["extension"][1]["valueString"]}", 
                "{patient["extension"][2]["valueString"]}",
                "{patient["extension"][3]["valueCode"]}", 
                "{patient["gender"]}", "{patient["extension"][4]["valueAddress"]["city"]}", "{patient["extension"][4]["valueAddress"]["state"]}", "{patient["extension"][4]["valueAddress"]["country"]}", "{patient["extension"][5]["valueDecimal"]}", "{patient["extension"][6]["valueDecimal"]}", "{patient["identifier"][1]["value"]}", "{patient["identifier"][2]["value"]}",
                "{driver}", "{passport}" , "{patient["telecom"][0]["value"]}", "{patient["address"][0]["line"][0]}", "{patient["address"][0]["city"]}", "{patient["address"][0]["country"]}", "{patient["maritalStatus"]["text"]}", "{patient["communication"][0]["language"]["text"]}" 
                )
                """)
                mydb.commit()
            except:
                continue

def export_csv():
    """
    Function to generate a excel file with the patients data
    :parameters: conn (Mysql Connector object) 
    """
    patient_df = pd.read_sql("select * from patients", connect())
    patient_df.to_excel("./static/patients.xlsx", index=False)


if __name__ == "__main__":
    if not connect():
        create_db()
        conn = connect()
    else:
        conn = connect()
    
    cursor = conn.cursor()
    create_table(conn)
    add_data(cursor, conn)






