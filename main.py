import os
import json
import mysql.connector

def connect():
     global mydb
     try:
          mydb = mysql.connector.connect(
          host='localhost',
          user='root',
          password='',
          database='patients_db',
          )
         
     except:
          mydb = mysql.connector.connect(
          host='localhost',
          user='root',
          password='',
          )

def drop_db():
     try:
          mycursor = mydb.cursor()
          mycursor.execute("DROP DATABASE patients_db")
     except:
          pass
     finally:
          mycursor = mydb.cursor()
          mycursor.execute("CREATE DATABASE patients_db")

def create_table():
     global mycursor
     mycursor = mydb.cursor()
     mycursor.execute("SHOW TABLES")
     myresult = mycursor.fetchall() 
     if myresult == []:
          print('criando')
          mycursor.execute("CREATE TABLE patients(patient_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, unique_ID VARCHAR(100), given_name VARCHAR(20), family_name VARCHAR(50), birth_date DATE, us_core_race VARCHAR(30), us_core_ethnicity VARCHAR(50), mothers_maiden_name VARCHAR(100), us_core_birthsex CHAR, gender VARCHAR(20), birth_city VARCHAR(50), birth_state VARCHAR(50), birth_country CHAR(2), disability_adjusted_life_years FLOAT(15), quality_adjusted_life_years FLOAT(15), medical_record_number VARCHAR(50), social_security_number VARCHAR(50), driver_license VARCHAR(50), passport_number VARCHAR(50), phone_number VARCHAR(20), address_line VARCHAR(50), address_city VARCHAR(50), address_country VARCHAR(2), marital_status CHAR, multiple_birth VARCHAR(255), language VARCHAR(10) )")

#TODO connect, check and create the db
#check if the table patients exist, then create a table
connect()
drop_db()
connect()
create_table()



#TODO open all the files to get the info
#acessing the data json
lista = []
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

     mycursor.execute(f'''INSERT INTO patients(`patient_id`, `unique_ID`, `given_name`, `family_name`,
     `birth_date`, `us_core_race`, `us_core_ethnicity`, `mothers_maiden_name`, `us_core_birthsex`, `gender`, `birth_city`,`birth_state`, `birth_country`, `disability_adjusted_life_years`, `quality_adjusted_life_years`, `medical_record_number`,`social_security_number`, `driver_license`, `passport_number`, `phone_number`, `address_line`, `address_city`, `address_country`, `marital_status`, `language`)
     VALUES(NULL, "{patient["id"]}", "{patient["name"][0]["given"][0]}", "{patient["name"][0]["family"]}",
       "{patient["birthDate"]}", "{patient["extension"][0]["extension"][1]["valueString"]}",
        "{patient["extension"][1]["extension"][1]["valueString"]}", 
        "{patient["extension"][2]["valueString"]}",
         "{patient["extension"][3]["valueCode"]}", 
         "{patient["gender"]}", "{patient["extension"][4]["valueAddress"]["city"]}", "{patient["extension"][4]["valueAddress"]["state"]}", "{patient["extension"][4]["valueAddress"]["country"]}", "{patient["extension"][5]["valueDecimal"]}", "{patient["extension"][6]["valueDecimal"]}", "{patient["identifier"][1]["value"]}", "{patient["identifier"][2]["value"]}",
         "{driver}", "{passport}" , "{patient["telecom"][0]["value"]}", "{patient["address"][0]["line"][0]}", "{patient["address"][0]["city"]}", "{patient["address"][0]["country"]}", "{patient["maritalStatus"]["text"]}", "{patient["communication"][0]["language"]["text"]}" 
         )
         ''')
     mydb.commit()



#TODO think a best way to show the data


#TODO export the data to csv file




