import os
import json
import mysql.connector


def connect():
     """
     Funtion to connect using mysql connector and verify if the db exists
     :return: conn (mysql.connector Object) or False if unable to connect
     """
     try:
          conn = mysql.connector.connect(
          host='localhost',
          user='root',
          password='',
          database='patients_db',
          )
          return conn
     except:
          return False

def create_db():
     """
     Function to create a DB
     """
     conn = mysql.connector.connect(
          host='localhost',
          user='root',
          password='',
          )
     mycursor = conn.cursor()
     mycursor.execute("CREATE DATABASE patients_db")
     mycursor.close()
     conn.close()


def create_table(mydb):
     """
     Function to create the tables if the tables don't exist in the DB
     :parameters: mydb (Mysql connection object) 
     """
     mycursor = mydb.cursor()
     mycursor.execute("SHOW TABLES")
     myresult = mycursor.fetchall() 
     if myresult == []:
          print('Creating DB...')
          mycursor.execute("CREATE TABLE patients(unique_ID VARCHAR(100) NOT NULL PRIMARY KEY, given_name VARCHAR(20), family_name VARCHAR(50), birth_date DATE, us_core_race VARCHAR(30), us_core_ethnicity VARCHAR(50), mothers_maiden_name VARCHAR(100), us_core_birthsex CHAR, gender VARCHAR(20), birth_city VARCHAR(50), birth_state VARCHAR(50), birth_country CHAR(2), disability_adjusted_life_years FLOAT(15), quality_adjusted_life_years FLOAT(15), medical_record_number VARCHAR(50), social_security_number VARCHAR(50), driver_license VARCHAR(50), passport_number VARCHAR(50), phone_number VARCHAR(20), address_line VARCHAR(50), address_city VARCHAR(50), address_country VARCHAR(2), marital_status CHAR, multiple_birth VARCHAR(255), language VARCHAR(10) )")
          mydb.commit()
          mycursor = mydb.cursor()
          mycursor.execute("CREATE TABLE patient_event (patient_event_id MEDIUMINT NOT NULL AUTO_INCREMENT, patient VARCHAR(100), "
          "event_data LONGTEXT, type VARCHAR(255), PRIMARY KEY(patient_event_id), FOREIGN KEY (patient) REFERENCES "
          "patients(unique_ID))")
          print('Successful')

def add_event(event, unique_id, cursor):
     """
     Function that receive the event and unique ID and insert to DB temporary
     :parameter: event
     :parameter: unique_id
     :parameter: cursor
     """
     resource_event = json.dumps(event["resource"]).replace('"', '""').replace("'", "''")
     cursor.execute(f"""INSERT INTO `patients_db`.`patient_event`(`patient_event_id`,`patient`,`event_data`,  `type`)VALUES (NULL, '{unique_id}', '{resource_event}', '{event["resource"]["resourceType"]}')""")


#function created to reduce program loading time by adding information according to what is being queried
def read_id(unique_id):
     """
     Function to read the JSON with the unique_id
     :parameters: unique_id (str) patient unique id
     :return: information about the patient 
     """
     conn = connect()
     mycursor = conn.cursor()
     for item in os.listdir('data'):
          if unique_id in item:
               item = os.path.join('data', item)
               with open(item, encoding='utf-8') as f:
                    patient = json.load(f)
                    unique_id = patient["entry"][0]["resource"]["id"]
                    for entry in patient["entry"]:
                         if entry["resource"]["resourceType"] != "Patient":
                              add_event(entry, unique_id, mycursor)
     conn.commit()

#TODO open all the files to get the info
#acessing the data json
def add_data(mycursor, mydb):
     """
     Function to add the JSON data into DB
     :parameter: mycursor (Mysql Cursor Object)
     :parameter: mydb (Mysql Connection Object) 
     """
     for item in os.listdir('data'):
          item = os.path.join('data', item)
          with open(item, encoding='utf-8') as f:
               patient = json.load(f)
          patient = patient["entry"][0]["resource"]
          #exception to driver license
          try:
               driver = patient["identifier"][3]["value"]
          except IndexError:
               driver = 'N/A'
          #exception to passaport
          try:
               passport = patient["identifier"][5]['value']
          except IndexError:
               passport = 'N/A'

          print(patient["name"][0]["given"][0])

          try:
               mycursor.execute(f'''INSERT INTO patients(`unique_ID`, `given_name`, `family_name`,
               `birth_date`, `us_core_race`, `us_core_ethnicity`, `mothers_maiden_name`, `us_core_birthsex`, `gender`, `birth_city`,`birth_state`, `birth_country`, `disability_adjusted_life_years`, `quality_adjusted_life_years`, `medical_record_number`,`social_security_number`, `driver_license`, `passport_number`, `phone_number`, `address_line`, `address_city`, `address_country`, `marital_status`, `language`)
               VALUES("{patient["id"]}", "{patient["name"][0]["given"][0]}", "{patient["name"][0]["family"]}",
               "{patient["birthDate"]}", "{patient["extension"][0]["extension"][1]["valueString"]}",
               "{patient["extension"][1]["extension"][1]["valueString"]}", 
               "{patient["extension"][2]["valueString"]}",
               "{patient["extension"][3]["valueCode"]}", 
               "{patient["gender"]}", "{patient["extension"][4]["valueAddress"]["city"]}", "{patient["extension"][4]["valueAddress"]["state"]}", "{patient["extension"][4]["valueAddress"]["country"]}", "{patient["extension"][5]["valueDecimal"]}", "{patient["extension"][6]["valueDecimal"]}", "{patient["identifier"][1]["value"]}", "{patient["identifier"][2]["value"]}",
               "{driver}", "{passport}" , "{patient["telecom"][0]["value"]}", "{patient["address"][0]["line"][0]}", "{patient["address"][0]["city"]}", "{patient["address"][0]["country"]}", "{patient["maritalStatus"]["text"]}", "{patient["communication"][0]["language"]["text"]}" 
               )
               ''')
               mydb.commit()
          except:
               continue


if __name__ == '__main__':
     if not connect():
          create_db()
          conn = connect()
     else:
          conn = connect()
          
     cursor = conn.cursor()
     create_table(conn)
     add_data(cursor, conn)






#TODO think a best way to show the data


#TODO export the data to csv file




