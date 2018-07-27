
# coding: utf-8

# In[1]:


#Queries the Dark Sky API each minute for 5 minutes and inputs weather data into MySQL database
import requests
import json
import urllib
import datetime
import mysql.connector
import pandas
import time

class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return(int(value))


#retrieves the contents of part of a string located between two parts of the same string: first and last
def find_between(string, first, last):
    start = string.index(first) + len(first)
    end = string.index(last, start)
    return string[start:end]

#retrieves cloud cover data from API response and converts to a percent
def calculate_cloud_cover():
    clouds = float(find_between(data, "cloudCover:", ","))
    clouds = clouds * 100
    clouds = int(clouds)
    clouds = str(clouds)
    return clouds

#retrieves humidity data from API response and converts to a percent
def calculate_humidity():
    percent = float(find_between(data, "humidity:", ","))
    percent = percent * 100
    percent = int(percent)
    return percent
    
#Converts the wind direction from a specific degree to a direction
def get_wind_direction():
    wind_degree = find_between(data, "windBearing:", ",")
    if ((348.75 < float(wind_degree)) and (float(wind_degree) <= 360)) or ((0 <= float(wind_degree)) and (float(wind_degree) < 11.25)):
        return "N"
    elif (11.25 <= float(wind_degree)) and (float(wind_degree) < 33.75):
        return "NNE"
    elif (33.75 <= float(wind_degree)) and (float(wind_degree) < 56.25):
        return "NE"
    elif (56.26 <= float(wind_degree)) and (float(wind_degree) < 78.75):
        return "ENE"
    elif (78.75 <= float(wind_degree)) and (float(wind_degree) < 101.25):
        return "E"
    elif (101.25 <= float(wind_degree)) and (float(wind_degree) < 123.75):
        return "ESE"
    elif (123.75 <= float(wind_degree)) and (float(wind_degree) < 146.25):
        return "SE"
    elif (146.25 <= float(wind_degree) < 168.75):
        return "SSE"
    elif (168.75 <= float(wind_degree) < 191.25):
        return "S"
    elif (191.25 <= float(wind_degree) < 213.75):
        return "SSW"
    elif (213.75 <= float(wind_degree) < 236.25):
        return "SW"
    elif (236.25 <= float(wind_degree) < 258.75):
        return "WSW"
    elif (258.75 <= float(wind_degree) < 281.25):
        return "W"
    elif (281.25 <= float(wind_degree) < 303.75):
        return "WNW"
    elif (303.75 <= float(wind_degree) < 326.25):
        return "NW"
    elif (326.25 <= float(wind_degree) <= 348.75):
        return "NNW"
    else:
        return "None"
    
#displays organized data from the API response    
def print_data():
    print("Time of Observation: " + now[1] + "/" + now[2] + "/" + now[0] + " at " + now[3] + ":" + now[4] + ":" + now[5])
    print("Latitude: " + find_between(data, "latitude:", ","))
    print("Longitude: " + find_between(data, "longitude:", ","))
    print("Weather: " + find_between(data, "summary:", ",icon"))
    print("Temperature: " + find_between(data, "temperature:", ",") + " F")
    print("Atmospheric Pressure: " + find_between(data, "ssure:", ",") + " mb")
    print("Humidity: " + str(calculate_humidity()) + " %")
    print("Dew Point: " + find_between(data, "Point:", ","))
    print("Visibility: " + find_between(data, "visibility:", ",") + " miles")
    print("Wind Speed: " + find_between(data, "Speed:", ",") + " miles/hour")
    print("Wind Direction: " + get_wind_direction())
    print("Percent Cloud Cover: " + calculate_cloud_cover() + " %")
    print("")
    print("Powered by Dark Sky")
    
#creates list of weather data points to be used to input data into mysql server    
def create_weather_list():
    Time_of_Observation = now[1] + "/" + now[2] + "/" + now[0] + " at " + now[3] + ":" + now[4] + ":" + now[5]
    Latitude = find_between(data, "latitude:", ",")
    Longitude = find_between(data, "longitude:", ",")
    Condition = find_between(data, "summary:", ",icon")
    Temperature = find_between(data, "temperature:", ",")
    Pressure = find_between(data, "ssure:", ",")
    Humidity = str(calculate_humidity())
    Dew_Point = find_between(data, "Point:", ",")
    Visibility = find_between(data, "visibility:", ",")
    Wind_Speed = find_between(data, "Speed:", ",")
    Wind_Direction = get_wind_direction()
    CloudCover = calculate_cloud_cover()
    weather_data_list = [Time_of_Observation, Latitude, Longitude, Condition, Temperature, Pressure, Humidity, Dew_Point, Visibility, Wind_Speed, Wind_Direction, CloudCover]       
    return weather_data_list

i = 0    
while (i < 5):
    #Checks if data has been retrieved from Dark Sky API
    queryok = 0
    while (queryok == 0):
        try:
            #retrieve weather data from Dark Sky and print data
            url = "https://api.darksky.net/forecast/apikey/35.73,-78.85?exclude=minutely,hourly,daily,alerts&units=us"
            response = urllib.request.urlopen(url)
            data = response.read().decode('utf-8')
        except:
            #Attempt to query Dark Sky again in 15 seconds
            print("Unable to query Dark Sky")
            time.sleep(15)
        else:
            for character in data:
                if character in '"':
                    data = data.replace(character,'')
    
            #convert from UTC timestamp to date and time
            timestamp = int(find_between(data, "time:", ","))
            current_time = datetime.datetime.fromtimestamp(timestamp)
            now = current_time.strftime('%Y %m %d %H %M %S')
            now = now.split(" ")
    
            #creates list of weather data points
            weather_data_list = create_weather_list()
            queryok = 1
    
    #Checks if connected to MySQL server
    connectok = 0
    while (connectok == 0):
        try:
            #connect to MySQL server
            cnx = mysql.connector.connect(host='localhost',database='weather',user='root',password='')
        except:
            #Attempt to connect to MySQL server again in 15 seconds
            print("Unable to connect to MySQL server")
            time.sleep(15)
        else:
            #Input weather data into MySQL database
            cursor = cnx.cursor()
            cursor.execute(
            '''INSERT INTO weather_info(Time_of_Observation, Latitude, Longitude, Weather_Condition, Temperature_F, Atmospheric_Pressure_mb, Percent_Humidity, Dew_Point, Visibility_Miles, Wind_Speed, Wind_Direction, Percent_Cloud_Cover)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
            (weather_data_list[0], weather_data_list[1], weather_data_list[2], weather_data_list[3], weather_data_list[4], weather_data_list[5], weather_data_list[6], weather_data_list[7], weather_data_list[8], weather_data_list[9], weather_data_list[10], weather_data_list[11]))
            cnx.commit()
            cursor.close()
            cnx.close()
            connectok = 1
    
    #increment the counter and wait 1 minute before next query
    i += 1
    time.sleep(60)


    

