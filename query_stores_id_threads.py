import pathlib
import json
import pycountry
import google_play_scraper as gplay
from itunes_app_scraper.scraper import AppStoreScraper
import csv 
from datetime import date
import time
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import pycountry


class app_instance:
    def __init__(self, name, id, store, country_code, genre):
        self.name = name
        self.id = id
        self.store = store
        self.country_code = country_code 
        self.genre = genre 
    
    def country_name(self):
        cc = pycountry.countries.get(alpha_2=self.country_code)
        cname = cc.name 
        return cname

def get_app_dict():
    #convert app_list.csv to .json
    csvFilePath = 'app_list_2020.csv'
    jsonFilePath = 'app_list_2020.json'
    path = pathlib.Path(jsonFilePath)

    if path.is_file():
        try:
            print(f"Opening app list file '{jsonFilePath}'")
            with open(jsonFilePath) as dfile:
                app_dict = json.load(dfile)
        except IOError:
            print(f"File '{jsonFilePath}' does not exist!")
    else:
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
        app_dict = data
        # Open a json writer, and use the json.dumps()
        # function to dump data
        with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
            jsonf.write(json.dumps(data, indent=4))
        print('Converting csv file to JSON!')
    return app_dict

def save_json(filename, data):
    # save dictionary data into a json file
    out_file = open(filename, "w") 
    json.dump(data, out_file, indent = 6) 
    out_file.close()

def save_csv(filename, data):
    # save list into a csv file to output folder
    #filename = f"appcensor-{timestamp}.csv"
    fieldnames = ['date','app_name', 'app_id', 'app_store', 'country_code', 'country', 'censored', 'genre', 'url']

    with open(filename, mode='w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)
        writer.writerows(data)

def app_object_gen():
    app_dict = get_app_dict()

    TEST_LIST=['af', 'al', 'dz', 'ao', 'ai', 'ag']
    COUNTRY_LIST=['af', 'al', 'dz', 'ao', 'ai', 'ag', 'ar', 'am', 'au', 'at', 'az', 'bs', 'bh', 'bb', 'by', 'be', 'bz', 'bj', 'bm', 'bt', 'bo', 
        'ba', 'bw', 'br', 'bn', 'bg', 'bf', 'cv', 'kh', 'cm', 'ca', 'ky', 'td', 'cl', 'cn', 'co', 'cg', 'cd', 'cr', 'ci', 'hr', 'cy',
         'cz', 'dk', 'dm', 'do', 'ec', 'eg', 'sv', 'ee', 'sz', 'fj', 'fi', 'fr', 'ga', 'gm', 'ge', 'de', 'gh', 'gr', 'gd', 'gt', 
        'gw', 'gy', 'hn', 'hk', 'hu', 'is', 'in', 'id', 'iq', 'ie', 'il', 'it', 'jm', 'jp', 'jo', 'kz', 'ke', 'kr', 'kw', 'kg', 'la', 
        'lv', 'lb', 'lr', 'ly', 'lt', 'lu', 'mo', 'mk', 'mg', 'mw', 'my', 'mv', 'ml', 'mt', 'mr', 'mu', 'mx', 'fm', 'md', 'mn', 'me', 
        'ms', 'ma', 'mz', 'mm', 'na', 'nr', 'np', 'nl', 'nz', 'ni', 'ne', 'ng', 'no', 'om', 'pk', 'pw', 'pa', 'pg', 'py', 'pe', 'ph', 
        'pl', 'pt', 'qa', 'ro', 'ru', 'rw', 'kn', 'lc', 'vc', 'st', 'sa', 'sn', 'rs', 'sc', 'sl', 'sg', 'sk', 'si', 'sb', 'za', 'es', 'lk', 'sr', 
        'se', 'ch', 'tw', 'tj', 'th', 'to', 'tt', 'tn', 'tr', 'tm', 'tc', 'ug', 'ua', 'ae', 'gb', 'us', 'uy', 'uz', 'vu', 've', 'vn', 'vg', 'ye', 'zm', 'zw']

    app_objects= []

    for country in COUNTRY_LIST:
        for i in app_dict.values():
            app_objects.append(app_instance(i['name'], i['aid'], 'apple', country, i['genre']))
            app_objects.append(app_instance(i['name'], i['gid'], 'google', country, i['genre']))
    
    return app_objects

def gplay_scraper(app):
    print(f"Querying Play Store for {app.name} in {app.country_name()}")

    try:
        result = gplay.app(app.id, country=app.country_code)
        #save_json(f"gplay/gplay_{app.name}_{app.country_name()}.json", result)
        data = [timestamp, app.name, app.id, app.store, app.country_code, app.country_name(), 'True', app.genre, result['url']]
        
    except:
        #print(f"App {app['name']} doesn't exist in {country}!")
        #save_json(f"gplay/gplay_{app.name}_{app.country_name()}.json", "[]")
        data = [timestamp, app.name, app.id, app.store, app.country_code, app.country_name(), 'False', app.genre, 'None']
    outputqueue.append(data)
    
def itunes_scraper(app):
    print(f"Querying Itunes App Store for {app.name} in {app.country_name()}")
    appstore = AppStoreScraper()
    try:
        result = appstore.get_app_details(app_id=app.id, country=app.country_code)
        #save_json(f"itunes/itunes_{app.name}_{app.country_name()}.json", result)
        data = [timestamp, app.name, app.id, app.store, app.country_code, app.country_name(), 'True', app.genre, result['trackViewUrl']]
        
    except:
        #print(f"App {app['name']} doesn't exist in {country}!")
        #save_json(f"itunes/itunes_{app.name}_{app.country_name()}.json", "[]")
        data = [timestamp, app.name, app.id, app.store, app.country_code, app.country_name(), 'False', app.genre, 'None']
    outputqueue.append(data)

def scraper_threading(app):
    if app.store == 'apple':
        itunes_scraper(app)
    elif app.store == 'google':
        gplay_scraper(app)
    else:
        print('No matching app store name')


if __name__=='__main__':
    t1 = time.perf_counter()
    ts = date.today()
    timestamp = ts.isoformat()

    #create output directory
    print("Creating directories")
    pathlib.Path("output").mkdir(parents=True, exist_ok=True)

    app_objects = app_object_gen() 
    
    outputqueue = deque()

    with ThreadPoolExecutor(max_workers=10) as exe:
        # Maps the method  scraper_threading with list of app_objects
        run = exe.map(scraper_threading, app_objects)
    
    rows = list(outputqueue)
    save_csv(f"output/appcense-{timestamp}.csv", rows)

    t2 = time.perf_counter()
    print(f'MultiThreaded Code Took:{t2 - t1} seconds')

    print('Job Completed!')



 

    



