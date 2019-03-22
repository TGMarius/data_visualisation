import pymongo
import csv
import pprint;
import numpy as np
import pandas as pd

import datetime

db = pymongo.MongoClient()['kulitta']

#csv file with uk cities and their population
df = pd.read_csv('uk_cities_population.csv')

#only consider cities with a population over 90k
over_onek = df['population'] > 100000 

test = df[over_onek]
cities = [c.lower() for c in test.city.tolist()]
#csv file titles
field_names = ["city", "year", "month", "attending_count", "interested_count", "noreply_count", "total_invited"]

z = []

 #Get all data which has an attending_count
    #Inviting people is always true
        #type is always public
        
csv_file = 'csv_devel_v62.csv'
        
print('Start Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))

for d in db['facebook_events'].find({ "attending_count" : { "$exists": True } }):
    if 'place' in d.keys() and 'location' in d['place'].keys() and 'city' in d['place']['location'].keys():
        if d['place']['location']['city'].lower().strip() in cities:
            d['city'] = d['place']['location']['city']

            if 'start_time' in d.keys():
                if '2015' in d['start_time'] or '2016' in d['start_time'] or '2017' in d['start_time']:
                    #Get each year indivdually
                    d['year'] = d['start_time']
                    year = d['year']
                    year_strip = d['year'][:4]
                    d['year'] = year_strip

                    #Get each month individually
                    d['month'] = d['start_time']
                    month = d['month']
                    month_strip = d['month'][5:]
                    d['month'] = month_strip[:2]

                    #Get sum of all atendees
                    if 'attending_count' in d.keys() and 'declined_count' in d.keys() and 'interested_count' in d.keys() and 'noreply_count' in d.keys():
                        d['total_invited'] = d['attending_count'] + d['declined_count'] + d['interested_count'] + d['noreply_count']

                        #ignore if number of invites is 0
                        if d['total_invited'] > 100:
                            row = [d[f] for f in field_names]   
                            #spamwriter.writerow(row) #change row to whatever df becomes.

                            print('Append Rows Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
                            z.append(row)
                            
df = pd.DataFrame(z)

print('Transposing Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))

df.transpose()

df.columns = ["city", "year", "month", "attending_count", "interested_count", "noreply_count", "total_invited"]

grouped_month = df.groupby(["city", "year", "month"]).sum()

print('Create Csv Timestamp: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))

grouped_month.to_csv(csv_file, sep=',', encoding='utf-8')

print('CSV File created: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))

print('Read CSV: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))

df = pd.read_csv(csv_file)

print('Calculate percentage: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))

df['mean'] = df[['attending_count','interested_count','noreply_count','total_invited']].mean(axis=1)
df['standard_deviation'] = df[['attending_count','interested_count','noreply_count','total_invited']].std(axis=1)
df['attending_percentage'] = df['attending_count'].astype(float) * (100/df['total_invited'].astype(float))


print('Replace CSV: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
df.to_csv(csv_file, sep=',', encoding='utf-8', index=False)

df.head(10)







                                    
                                    

