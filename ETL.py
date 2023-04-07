import requests
import datetime
import os
import sqlite3
import openrefine_api as api

dir = r'./temp' 

proj_nimi = (input("Sisestage uue projekti nimi : "))

baasi_nimi = (input("Sisestage SQLite andmebaasi nimi(vaikimisis: ETL.sqlite3) : ") or "ETL.sqlite3")

faili_formaat = (input("Valige andmefaili formaat(vaikimisi: .csv) : ") or "csv")

history_fail = (input("Valige Openrefain history fail(vaikimisi: history.json) : ") or "history.json")

or_server_url_port=(input("Openrefine serveri url:port(vaikimisi: http://127.0.0.1:3333/) : ") or "http://127.0.0.1:3333/")

input("Vajalik on Openrefin programmi käivitamine! (Enter)")

projekt_start = (input("Jätkame projektiga?(Y/N) ") or "Y")
if projekt_start == "N":
    exit()

start_time = datetime.datetime.now()

if not os.path.exists(dir):
    os.makedirs(dir)

#Fetch data from web

print("Laeme andmed veebilehelt")

def fetch_air_range(station_id, date_from, date_until):
    url = 'http://airviro.klab.ee/station/csv'
    data = {
        'filter[type]': 'INDICATOR',
        'filter[cancelSearch]': '',
        'filter[stationId]': station_id,
        'filter[dateFrom]': date_from,
        'filter[dateUntil]': date_until,
        'filter[submitHit]': '1',
        'filter[indicatorIds]': ''
    }
    response = requests.post(url, data)
    return response.text

def get_first_and_last_day_of_month(year, month):
    # Get the first day of the month
    first_day = datetime.date(year, month, 1)

    # Get the number of days in the month
    if month == 12:
        num_days = 31
    else:
        num_days = (datetime.date(year, month+1, 1) - datetime.timedelta(days=1)).day

    # Get the last day of the month
    last_day = datetime.date(year, month, num_days)

    return first_day, last_day

for month in range(1, 13):
    first_day, last_day = get_first_and_last_day_of_month(2022, month)
    #print(first_day, last_day)

    data = fetch_air_range(8, first_day, last_day)
    with open(f'./temp/air{month}.csv','w',encoding='utf-8-sig') as f:
        f.write(data)

data = []
for month in range(1, 13):
    with open(f'./temp/air{month}.csv', 'r') as f:
        if month == 1:
            header = f.readline()
            data.append(header)
        else:
            f.readline()
        data.extend(f.readlines())

with open('./air_2022.csv', 'w') as f:
    f.writelines(data)

for f in os.listdir(dir):
    os.remove(os.path.join(dir, f))

print("Andmed on veebilehelt alla laetud")


#Openrefian API

print("Openrefine operatsioonidega alustatud")

projekt_id=api.create_from_upload("air_2022.csv", proj_nimi)

api.apply_operations(projekt_id, history_fail)

api.export_to_file(projekt_id, faili_formaat)

delete_or_project = (input("Kustutame ajutiselt loodud Openrefine projekti?(Y/N) ") or "Y")
if delete_or_project == "Y":
    api.delete_project(projekt_id)

print("Openrefine operatsioonidega lõpetatud")


#SQLite database

print("SQLite andmebaasi loomine")

conn = sqlite3.connect(baasi_nimi)

c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS algsed_andmed (Kuupäev TEXT, SO2 REAL, NO2 REAL, CO REAL, O3 REAL, PM10 REAL, PM2point5 REAL, TEMP REAL, WD10 INT, WS10 REAL)""")

print("Andmete importimine andmebaasi")

with open('exported_file.csv','r',encoding='utf-8-sig') as file:
    no_records=0
    next(file)
    for row in file:
        c.execute("INSERT INTO algsed_andmed VALUES (?,?,?,?,?,?,?,?,?,?)",row.split(","))
        conn.commit()
        no_records += 1

print("Andemebaasi kirjutatud ridu: ", no_records)

c.execute("""CREATE TABLE IF NOT EXISTS paevakeskmine AS SELECT strftime('%m-%d',Kuupäev) Day, AVG(SO2), AVG(NO2), AVG(CO), AVG(O3), AVG(PM10),AVG(PM2point5),AVG(TEMP),AVG(WD10),AVG(WS10)
FROM algsed_andmed
GROUP by Day;""")

c.execute("""CREATE TABLE IF NOT EXISTS kuukeskmine AS SELECT strftime('%m',Kuupäev) Month, AVG(SO2), AVG(NO2), AVG(CO), AVG(O3), AVG(PM10),AVG(PM2point5),AVG(TEMP),AVG(WD10),AVG(WS10)
FROM algsed_andmed
GROUP by Month;""")  

print("Tabelid andmebaasi loodud")

conn.close()

os.remove("air_2022.csv")
os.remove("exported_file.csv")
os.rmdir(dir)

print("Ajutised failid kustutatud")

end_time = datetime.datetime.now()
aeg=end_time - start_time

print(" ")

message = f"KOKKUVÕTE: \n"\
    f"------------------------------------\n"\
    f"Kuupäev: {datetime.datetime.now()} \n"\
    f"Projekti nimi: {proj_nimi} \n"\
    f"Sisend failiformaat: {faili_formaat} \n"\
    f"Operefine operations fail: {history_fail} \n"\
    f"Loodud SQLite andmebaas: {baasi_nimi} \n"\
    f"Töödeldud ridade arv: {no_records} \n"\
    f"Kulunud aeg: {aeg}"

print(message)
