from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
import collections
from math import sin, cos, sqrt, atan2, radians
import pandas as pd
import numpy as np
from haversine import haversine



spark = SparkSession.builder.config("spark.sql.warehouse.dir", "doulkeridis").appName("SparkSQL").getOrCreate()



def mapper(line):
    fields = line.split('|')
    return Row(restaurant_id=int(fields[0]), restaurant_name=str(fields[1].encode("utf-8")), lat=float(fields[3]), lon=float(fields[4])) 

def mapper2(line2):
    fields = line2.split('|')
    return Row(hotel_id=int(fields[0]), hotel_name=str(fields[1].encode("utf-8")), lat=float(fields[4]), lon=float(fields[5])) 


lines = spark.sparkContext.textFile("../../data/doulkeridis/restaurants-exmpl-250.txt"
# , 100
)
restaurantsDataset = lines.map(mapper)

line2 = spark.sparkContext.textFile("../../data/doulkeridis/hotels-exmpl-250.txt"
# , 100
)
hotelsDataset = line2.map(mapper2)

# Infer the schema, and register the DataFrame as a table.
schemaRestaurantsDataset = spark.createDataFrame(restaurantsDataset).cache()
schemaRestaurantsDataset.createOrReplaceTempView("restaurantsDataset")

schemaHotelsDataset = spark.createDataFrame(hotelsDataset).cache()
schemaHotelsDataset.createOrReplaceTempView("hotelsDataset")



# SQL can be run over DataFrames that have been registered as a table.
restaurants = spark.sql("SELECT restaurant_name, lat, lon FROM restaurantsDataset")

restaurants = restaurants.repartition(150, "lat", "lon")
# restaurants = restaurants.repartition(10, "lat", "lon")
# print("######## restaurants partitions ###########")
# print(restaurants.rdd.getNumPartitions())
# print(restaurants.show())
hotels = spark.sql("SELECT  hotel_name, lat, lon FROM hotelsDataset")
hotels = hotels.repartition(150, "lat", "lon")
# print("################### hotel partitions ################")
# print(hotels.rdd.getNumPartitions())
# print(hotels.show())
restaurants.write.mode("overwrite").csv("rest_example.txt")
hotels.write.mode("overwrite").csv("hotel_example.txt")


# The results of SQL queries are RDDs and support all the normal RDD operations.
# print("---------- RESTAURANTS ---------")
# for line in restaurants.collect():
#   print(line)

# print(hotels.show())

# print("---------- HOTELS ---------")
# for line in hotels.collect():
#       print(line)

distances_table = []
for restaurant in restaurants.collect():
      max_distance = 0.5
      R = 6373.0
      restaurant_name = restaurant[0]
      restaurant_lat = restaurant[1]
      restaurant_lon = restaurant[2]
      
      for hotel in hotels.collect():
            row = []
            hotel_name = hotel[0]
            hotel_lat = hotel[1]
            hotel_lon = hotel[2]

            distance = haversine((restaurant_lat, restaurant_lon),(hotel_lat, hotel_lon))


            if distance < max_distance:
                  row.append(restaurant_name)
                  row.append(hotel_name)
                  row.append(round(distance, 2))
                  distances_table.append(row)



distances_DF = pd.DataFrame(distances_table,columns=['Restaurant Name', 'Hotel Name', 'Distance'])
print(distances_DF)
# distances_DF.to_csv("./distances.csv",index=False)



spark.stop()

