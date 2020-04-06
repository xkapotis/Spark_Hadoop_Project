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

lines = spark.sparkContext.textFile("../../data/doulkeridis/restaurants-ver1.txt"
, 100
)
restaurantsDataset = lines.map(mapper)

line2 = spark.sparkContext.textFile("../../data/doulkeridis/hotels-ver1.txt"
, 100
)
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
restaurants_name_mapper = restaurants.rdd.map(lambda x : (x[0], x[1], ((x[2]+90)*180+x[3]), x[2], x[3]))
df_restaurants_name_mapper = restaurants_name_mapper.toDF()
df_restaurants_name_mapper_final = df_restaurants_name_mapper.withColumn("_3", df_restaurants_name_mapper["_3"].cast(IntegerType()))

restaurants_name_mapper_final_cut_one = df_restaurants_name_mapper_final.rdd.map(lambda x : (x[0], x[1], (x[2]/100), x[3], x[4]))
df_restaurants_name_mapper_final_cut_one = restaurants_name_mapper_final_cut_one.toDF()
df_restaurants_name_mapper_final_cut_one_rounded = df_restaurants_name_mapper_final_cut_one.withColumn("_3", df_restaurants_name_mapper_final_cut_one["_3"].cast(IntegerType()))

hotels_name_mapper = hotels.rdd.map(lambda x: (x[0],x[1], ((x[2]+90)*180+x[3]), x[2], x[3]))
df_hotels_name_mapper = hotels_name_mapper.toDF()
df_hotels_name_mapper_final = df_hotels_name_mapper.withColumn("_3", df_hotels_name_mapper["_3"].cast(IntegerType()))

hotels_name_mapper_final_cut_one = df_hotels_name_mapper_final.rdd.map(lambda x : (x[0], x[1], (x[2]/100), x[3], x[4]))
df_hotels_name_mapper_final_cut_one = hotels_name_mapper_final_cut_one.toDF()
df_hotels_name_mapper_final_cut_one_rounded = df_hotels_name_mapper_final_cut_one.withColumn("_3", df_hotels_name_mapper_final_cut_one["_3"].cast(IntegerType()))


restaurants_with_columnId = df_restaurants_name_mapper_final_cut_one_rounded.withColumn("dataFrameId", lit("a"))
hotels_with_columnId = df_hotels_name_mapper_final_cut_one_rounded.withColumn("dataFrameId", lit("b"))
# print(restaurants_with_columnId.show())
# print(hotels_with_columnId.show())

restaurants_ordered_by_Hash = restaurants_with_columnId.orderBy("_3", ascending= True)
hotels_ordered_by_Hash = hotels_with_columnId.orderBy("_3", ascending= True)
# print(restaurants_ordered_by_Hash.show())
# print(hotels_ordered_by_Hash.show())


#######################################################
########### PARTITIONING ##############################
#######################################################

########  Partitioning Restaurants ############
restaurants_partitioned = restaurants_ordered_by_Hash.repartitionByRange(10, col("_3"))
# print(restaurants_partitioned.show())
# print(restaurants_partitioned.rdd.getNumPartitions())
# restaurants_partitioned.write.mode("overwrite").csv("apotelesmata/results.txt")
restaurants_partitioned_df = restaurants_partitioned.toDF("restaurantId", "restaurantName", "restaurantHashKey", "restaurantLat", "restaurantLon", "restaurantsDataFrameId")

########  Partitioning Hotels ############
hotels_partitioned = hotels_ordered_by_Hash.repartitionByRange(10, col("_3"))
# print(hotels_partitioned.show())
# print(hotels_partitioned.rdd.getNumPartitions())
# hotels_partitioned.write.mode("overwrite").csv("apotelesmata/results.txt")
hotels_partitioned_df = hotels_partitioned.toDF("hotelId", "hotelName", "hotelHashKey", "hotelLat", "hotelLon", "hotelDataFrameId")

joined_by_Hash = restaurants_partitioned_df.join(hotels_partitioned_df ,hotels_partitioned_df['hotelHashKey'] == restaurants_partitioned_df['restaurantHashKey'] ,how = 'full')
# print(joined_by_Hash.show())

#### Drop the rows with nulls Because in this position will not exist either hotel or restaurnt #####
joined_cleaned = joined_by_Hash.na.drop()
# print(joined_cleaned.show())

distances = joined_cleaned.rdd.map(lambda x:(x[0], x[1], x[6], x[7], haversine(( x[3] , x[4] ),( x[9] , x[10] )) ))
distances_DF = distances.toDF()
# print(distances_DF.show())

results = distances_DF.filter(distances_DF[4] < 0.5)
print(results.show())

##### for the full set of restaurants-hotels and the distances 
# for row in results.collect():
#     print(row)




spark.stop()


