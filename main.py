import os
import json
import mysql.connector


#TODO connect, check and create the db
try:
     mydb = mysql.connector.connect(
     host='localhost',
     user='root',
     password='',
     database='testes',
     )
     print('Connected...')
except:
     mydb = mysql.connector.connect(
     host='localhost',
     user='root',
     password='',
     )
     mycursor = mydb.cursor()
     mycursor.execute("CREATE DATABASE testes")
     print('Created...')


#TODO open all the files to get the info
#acessing the data json

# for item in os.listdir('data'):
#      item = os.path.join('data', item)
#      with open(item, encoding='utf-8') as f:
#           patient = json.load(f)
#      patient = patient["entry"][0]["resource"]
#      if patient['id'] in lista: #just for check the unique ID
#           print('error!!')
#      else:
#           lista.append(patient['id'])
#           print(patient["communication"])

#TODO think a best way to show the data


#TODO export the data to csv file




