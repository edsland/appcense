import pathlib
import google_play_scraper as gplay
from itunes_app_scraper.scraper import AppStoreScraper
import json
import csv 
from pprint import pprint
from datetime import datetime
import pycountry
import time

appstore = AppStoreScraper()



#create directories
print("Creating directories")
directories=["gplay", "itunes", "output"]

for directory in directories:
    pathlib.Path(f"{directory}").mkdir(parents=True, exist_ok=True)


#Turn csv app list to json
def csv2json(csvFilePath, jsonFilePath):
     
    # create a dictionary
    data = {}
     
    # Open a csv reader called DictReader
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
         
        # Convert each row into a dictionary
        # and add it to data
        for rows in csvReader:
            # Assuming a column named 'name' to be the primary key
            key = rows['name']
            data[key] = rows
 
    # Open a json writer, and use the json.dumps()
    # function to dump data
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))

def save_json(filename, data):
    # the json file where the output must be stored 
    out_file = open(filename, "w") 
    json.dump(data, out_file, indent = 6) 
    out_file.close()

def save_csv(data):
    now = datetime.now()
    filename = f"appcensor-{now.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    fieldnames = ['timestamp','name', 'id', 'store', 'country_code', 'country', 'available', 'genre', 'url']

    with open(f'output/{filename}', mode='w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)
        writer.writerows(data)

def app_scrapper():
    csvFilePath = 'app_list_2020.csv'
    jsonFilePath = 'app_list_2020.json'
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d-%H-%M-%S')

    
    #country list
    #ISO3166CC=['ne', 'ng', 'nf', 'mp', 'no']
    COUNTRY_LIST=['af', 'al', 'dz', 'ao', 'ai', 'ag', 'ar', 'am', 'au', 'at', 'az', 'bs', 'bh', 'bb', 'by', 'be', 'bz', 'bj', 'bm', 'bt', 'bo', 
    'ba', 'bw', 'br', 'io', 'bn', 'bg', 'bf', 'cv', 'kh', 'cm', 'ca', 'ky', 'td', 'cl', 'cn', 'cx', 'cc', 'co', 'cg', 'cd', 'cr', 'ci', 'hr', 
    'cw', 'cy', 'cz', 'dk', 'dm', 'do', 'ec', 'eg', 'sv', 'ee', 'sz', 'fj', 'fi', 'fr', 'ga', 'gm', 'ge', 'de', 'gh', 'gr', 'gd', 'gu', 'gt', 
    'gw', 'gy', 'hm', 'hn', 'hk', 'hu', 'is', 'in', 'id', 'iq', 'ie', 'il', 'it', 'jm', 'jp', 'jo', 'kz', 'ke', 'ki', 'kr', 'kw', 'kg', 'la', 
    'lv', 'lb', 'lr', 'ly', 'lt', 'lu', 'mo', 'mk', 'mg', 'mw', 'my', 'mv', 'ml', 'mt', 'mh', 'mr', 'mu', 'yt', 'mx', 'fm', 'md', 'mn', 'me', 
    'ms', 'ma', 'mz', 'mm', 'na', 'nr', 'np', 'nl', 'nz', 'ni', 'ne', 'ng', 'nf', 'mp', 'no', 'om', 'pk', 'pw', 'pa', 'pg', 'py', 'pe', 'ph', 
    'pl', 'pt', 'qa', 'ro', 'ru', 'rw', 'kn', 'lc', 'vc', 'st', 'sa', 'sn', 'rs', 'sc', 'sl', 'sg', 'sk', 'si', 'sb', 'za', 'es', 'lk', 'sr', 
    'se', 'ch', 'tw', 'tj', 'th', 'to', 'tt', 'tn', 'tr', 'tm', 'tc', 'ug', 'ua', 'ae', 'gb', 'us', 'um', 'uy', 'uz', 'vu', 've', 'vn', 'vg', 'ye', 'zm', 'zw']

    path = pathlib.Path(jsonFilePath)
    
    if path.is_file():
        try:
            print(f"Opening app list file '{jsonFilePath}'")
            with open(jsonFilePath) as dfile:
                app_dict = json.load(dfile)
        except IOError:
            print(f"File '{jsonFilePath}' does not exist!")
    else:
        csv2json(csvFilePath, jsonFilePath)
        print('Converting csv file to JSON!')
        try:
            with open(jsonFilePath) as dfile:
                app_dict = json.load(dfile)
        except IOError:
            print(f"File '{jsonFilePath}' not accessible")

    csvrows = []
    for app in app_dict.values():
        for country in COUNTRY_LIST:
            print(f"Querying play store and itunes for {app['name']} in {country}")
            cc = pycountry.countries.get(alpha_2=country)
            country_name = cc.name

            try:
                
                gresult = gplay.app(app['gid'], country=country)
                save_json(f"gplay/gplay_{app['name']}_{country}.json", gresult)
                
                rdata = [timestamp, app['name'], app['gid'], 'google', country, country_name, 'True', app['genre'], gresult['url']]
                #csvrows.append(rdata)
            except:
                #print(f"App {app['name']} doesn't exist in {country}!")
                save_json(f"gplay/gplay_{app['name']}_{country}.json", "[]")
                
                rdata = [timestamp, app['name'], app['gid'], 'google', country, country_name, 'False', app['genre'], 'None']
                csvrows.append(rdata)

            try:
                aresult = appstore.get_app_details(app_id=app['aid'], country=country)
                save_json(f"itunes/itunes_{app['name']}_{country}.json", aresult)

                rdata = [timestamp, app['name'], app['aid'], 'apple', country, country_name, 'True', app['genre'], aresult['trackViewUrl']]
                csvrows.append(rdata)
            except:
                #print(f"App {app['name']} doesn't exist in {country}!")
                save_json(f"itunes/itunes_{app['name']}_{country}.json", "[]")

                rdata = [timestamp, app['name'], app['aid'], 'apple', country, country_name, 'False', app['genre'], 'None']
                csvrows.append(rdata)
    
    print('Saving result to csv file')

    save_csv(csvrows)

print(f"started at {time.strftime('%X')}")
app_scrapper()
print(f"finished at {time.strftime('%X')}")
