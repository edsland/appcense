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

