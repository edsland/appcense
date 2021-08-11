import pathlib
import google_play_scraper as gplay
from itunes_app_scraper.scraper import AppStoreScraper
import json
import csv 
from pprint import pprint
from datetime import datetime
import pycountry
import time
import asyncio


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
    fieldnames = ['timestamp','name', 'id', 'store', 'country_code', 'country', 'available', 'genre', 'url']

    with open(filename, mode='w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)
        writer.writerows(data)

def gplay_scrapper(cc_tup, app):
    country = cc_tup[0]
    country_name = cc_tup[1]
    print(f"Querying Play Store for {app['name']} in {country_name}")

    try:
        gresult = gplay.app(app['gid'], country=country)
        save_json(f"gplay/gplay_{app['name']}_{country}.json", gresult)
        
        rdata = [timestamp, app['name'], app['gid'], 'google', country, country_name, 'True', app['genre'], gresult['url']]
        csvrows.append(rdata)
    except:
        #print(f"App {app['name']} doesn't exist in {country}!")
        save_json(f"gplay/gplay_{app['name']}_{country}.json", "[]")
        
        rdata = [timestamp, app['name'], app['gid'], 'google', country, country_name, 'False', app['genre'], 'None']
        csvrows.append(rdata)
    return csvrows

def itunes_scrapper(cc_tup, app):
    country = cc_tup[0]
    country_name = cc_tup[1]
    print(f"Querying Itunes App Store for {app['name']} in {country_name}")

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


def app_scapper():
    app_dict = get_app_dict()

    #country list
    #ISO3166CC=['ne', 'ng', 'nf']
    COUNTRY_LIST=['af', 'al', 'dz', 'ao', 'ai', 'ag', 'ar', 'am', 'au', 'at', 'az', 'bs', 'bh', 'bb', 'by', 'be', 'bz', 'bj', 'bm', 'bt', 'bo', 
    'ba', 'bw', 'br', 'io', 'bn', 'bg', 'bf', 'cv', 'kh', 'cm', 'ca', 'ky', 'td', 'cl', 'cn', 'cx', 'cc', 'co', 'cg', 'cd', 'cr', 'ci', 'hr', 
    'cw', 'cy', 'cz', 'dk', 'dm', 'do', 'ec', 'eg', 'sv', 'ee', 'sz', 'fj', 'fi', 'fr', 'ga', 'gm', 'ge', 'de', 'gh', 'gr', 'gd', 'gu', 'gt', 
    'gw', 'gy', 'hm', 'hn', 'hk', 'hu', 'is', 'in', 'id', 'iq', 'ie', 'il', 'it', 'jm', 'jp', 'jo', 'kz', 'ke', 'ki', 'kr', 'kw', 'kg', 'la', 
    'lv', 'lb', 'lr', 'ly', 'lt', 'lu', 'mo', 'mk', 'mg', 'mw', 'my', 'mv', 'ml', 'mt', 'mh', 'mr', 'mu', 'yt', 'mx', 'fm', 'md', 'mn', 'me', 
    'ms', 'ma', 'mz', 'mm', 'na', 'nr', 'np', 'nl', 'nz', 'ni', 'ne', 'ng', 'mp', 'no', 'om', 'pk', 'pw', 'pa', 'pg', 'py', 'pe', 'ph', 
    'pl', 'pt', 'qa', 'ro', 'ru', 'rw', 'kn', 'lc', 'vc', 'st', 'sa', 'sn', 'rs', 'sc', 'sl', 'sg', 'sk', 'si', 'sb', 'za', 'es', 'lk', 'sr', 
    'se', 'ch', 'tw', 'tj', 'th', 'to', 'tt', 'tn', 'tr', 'tm', 'tc', 'ug', 'ua', 'ae', 'gb', 'us', 'um', 'uy', 'uz', 'vu', 've', 'vn', 'vg', 'ye', 'zm', 'zw']

    global csvrows
    csvrows = []

    for app in app_dict.values():
        for country in COUNTRY_LIST:
            cc = pycountry.countries.get(alpha_2=country)
            country_name = cc.name 
            cc_tup = (country, country_name)

            gplay_scrapper(cc_tup, app)
            itunes_scrapper(cc_tup, app)
    
    print('Saving result to csv file')
    save_csv(f"output/{timestamp}-appcensor.csv", csvrows)


if __name__=='__main__':

    appstore = AppStoreScraper()
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d-%H-%M-%S')

    #create directories
    print("Creating directories")
    directories=["gplay", "itunes", "output"]

    for directory in directories:
        pathlib.Path(f"{directory}").mkdir(parents=True, exist_ok=True)

    app_scapper()
    



 




    



