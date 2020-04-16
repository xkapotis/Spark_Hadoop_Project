from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.functions import lit
import collections
from pyspark.sql.functions import split
from math import sin, cos, sqrt, atan2, radians
import pandas as pd
import numpy as np
from pyspark.sql.functions import pow, lit
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, collect_list, struct
from pyspark.sql.functions import *
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from haversine import haversine


spark = SparkSession.builder.config("spark.sql.warehouse.dir", "doulkeridis").appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split('|')
    return Row(id=int(fields[0]), names=str(fields[1].encode("utf-8")), lat=float(fields[3]), lon=float(fields[4])) 

def mapper2(line2):
    fields = line2.split('|')
    return Row(id=int(fields[0]), names=str(fields[1].encode("utf-8")), lat=float(fields[4]), lon=float(fields[5])) 

lines = spark.sparkContext.textFile("../../data/doulkeridis/restaurants-ver1.txt", 10)
restaurantsDataset = lines.map(mapper)

line2 = spark.sparkContext.textFile("../../data/doulkeridis/hotels-ver1.txt", 10)
hotelsDataset = line2.map(mapper2)

# Infer the schema, and register the DataFrame as a table.
schemaRestaurantsDataset = spark.createDataFrame(restaurantsDataset).cache()
schemaRestaurantsDataset.createOrReplaceTempView("restaurantsDataset")

schemaHotelsDataset = spark.createDataFrame(hotelsDataset).cache()
schemaHotelsDataset.createOrReplaceTempView("hotelsDataset")

# SQL can be run over DataFrames that have been registered as a table.
restaurants = spark.sql("SELECT id, names, lat, lon FROM restaurantsDataset")
hotels = spark.sql("SELECT  id, names, lat, lon FROM hotelsDataset")

# print(restaurants.show())
# print(hotels.show())


# ####### ADD the Hash Key as Column ######
# ####### ADD the Hash Key as Column ######
restaurants_name_mapper = restaurants.rdd.map(lambda x : (x[0], x[1], (int(int((x[2]+90)*180+x[3]) / 100)), x[2], x[3]))
df_restaurants_name_mapper = restaurants_name_mapper.toDF()

hotels_name_mapper = hotels.rdd.map(lambda x: (x[0],x[1], (int(int((x[2]+90)*180+x[3]) / 100)), x[2], x[3]))
df_hotels_name_mapper = hotels_name_mapper.toDF()


restaurants_with_columnId = df_restaurants_name_mapper.withColumn("dataFrameId", lit("a"))
hotels_with_columnId = df_hotels_name_mapper.withColumn("dataFrameId", lit("b"))
# print(restaurants_with_columnId.show())
# print(hotels_with_columnId.show())


#### Finaly the order will be by Lat it gives me better results the partitioning by lat 
restaurants_ordered_by_Lat = restaurants_with_columnId.orderBy("_4", ascending= True)
hotels_ordered_by_Lat = hotels_with_columnId.orderBy("_4", ascending= True)
# print(restaurants_ordered_by_Hash.show())
# print(hotels_ordered_by_Hash.show())


#######################################################
########### PARTITIONING ##############################
#######################################################

########  Partitioning Restaurants ############
restaurants_partitioned = restaurants_ordered_by_Lat.repartitionByRange(8, col("_4"))
# print(restaurants_partitioned.show())
# print(restaurants_partitioned.rdd.getNumPartitions())
# restaurants_partitioned.write.mode("overwrite").csv("apotelesmata/results.txt")

# print("-------- print the length of each partition for restaurants partitions -------------------")
# rdd_rests = restaurants_partitioned.rdd
# print(rdd_rests.glom().map(len).collect())

restaurants_partitioned_df = restaurants_partitioned.toDF("restaurantId", "restaurantName", "restaurantHashKey", "restaurantLat", "restaurantLon", "restaurantsDataFrameId")

########  Partitioning Hotels ############
hotels_partitioned = hotels_ordered_by_Lat.repartitionByRange(8, col("_4"))
# print(hotels_partitioned.show())
# print(hotels_partitioned.rdd.getNumPartitions())
# hotels_partitioned.write.mode("overwrite").csv("apotelesmata/results.txt")

# print("-------- print the length of each partition for hotel partitions -------------------")
# rdd_hotels = hotels_partitioned.rdd
# print(rdd_hotels.glom().map(len).collect())

hotels_partitioned_df = hotels_partitioned.toDF("hotelId", "hotelName", "hotelHashKey", "hotelLat", "hotelLon", "hotelDataFrameId")

joined_by_Hash = restaurants_partitioned_df.join(hotels_partitioned_df ,hotels_partitioned_df['hotelHashKey'] == restaurants_partitioned_df['restaurantHashKey'] ,how = 'full')
# print(joined_by_Hash.show())

#### Drop the rows with nulls Because in this position will not exist either hotel or restaurnt #####
joined_cleaned = joined_by_Hash.na.drop()
# print(joined_cleaned.show())

distances = joined_cleaned.rdd.map(lambda x:(x[0], x[1], x[6], x[7], haversine(( x[3] , x[4] ),( x[9] , x[10] )) ))
distances_DF = distances.toDF()
# print(distances_DF.show())

#### Print the algorithm's complexity ###
# print("Print the algorithm's complexity")
# print(distances_DF.collect())

results = distances_DF.filter(distances_DF[4] < 1.5)
# print(results.show().count())


######## haw many restaurant-hotel couples there are
print(results.count())


##### for the full set of restaurants-hotels and the distances 
# final_table = []
# for row in results.collect():
#     final_table.append(row)

# distances_DF = pd.DataFrame(final_table,columns=['Restaurant Id','Restaurant Name','Hotel Id','Hotel Name', 'Distance'])
# print(distances_DF)
# distances_DF.to_csv("./distances_Final_Result_Full_Texts.csv",index=False)


spark.stop()


