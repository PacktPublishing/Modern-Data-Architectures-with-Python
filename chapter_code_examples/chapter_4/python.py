# bathc processing
from faker import Faker
import pandas as pd
import random
fake = Faker()
def generate_data(num):
    row = [{"name":fake.name(),
               "address":fake.address(),
               "city":fake.city(),
               "state":fake.state(),
               "date_time":fake.date_time(),
               "friend":fake.catch_phrase()} for x in range(num)]
    return row
panda = pd.DataFrame(generate_data(10000))
fake_data = spark.createDataFrame(panda)

%sh
rm -rf /dbfs/tmp/csv_fake_data
rm -rf /dbfs/tmp/json_fake_data
rm -rf /dbfs/tmp/parquet_fake_data


fake_data.write.format("csv").save("dbfs:/tmp/csv_fake_data")

fake_data.write.format("json").save("dbfs:/tmp/json_fake_data")
fake_data.write.format("parquet").save("dbfs:/tmp/parquet_fake_data")

# Partitioning

database_name = "chapter4"
spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE ;")
spark.sql(f" CREATE SCHEMA IF NOT EXISTS {database_name};")
df.write.partition("state").mode("overwrite").format("delta").saveAsTable(f"{database_name}.delta_fake__bucket_data")


# Data Skew

df = df.repartition(10, "state")

from pyspark.sql.functions rand()
df = df.withColumn('salt', rand())
df = df.repartition(100, 'salt')

# reading data
df = spark.read.load("dbfs:/tmp/parquet_fake_data", format="parquet")


# Spark Schemas

from  pyspark.sql.types import StringType,DoubleType, StructField

from  pyspark.sql.types import StringType,DoubleType, StructField, StructType,IntegerType, ArrayType,MapType, LongType


schema_1 = StructType([StructField("my_string", StringType(), True),
                     StructField("my_double", DoubleType(), True),
                     StructField("sub_field", StructType([
                           StructField("my_name", StringType(), True)
                       ]), True)])


schema_2 = StructType([StructField("my_string", StringType(), True),
                     StructField("my_double", DoubleType(), True),
                       StructField("sub_field", StructType([
                           StructField("my_name", StringType(), True)
                       ]), True)])

schema_1.fields
[StructField('my_string', StringType(), True),
 StructField('my_double', DoubleType(), True),
 StructField('sub_field', StructType([StructField('my_name', StringType(), True)]), True)]

schema_array = StructType([
  StructField("id", IntegerType()),
  StructField("users", ArrayType(
      StructType([
          StructField("name", StringType()),
          StructField("address", StringType())
      ])
   )
])

schema_map = StructType([
        StructField('address', StringType(), True),
        StructField('id', LongType(), True),
        StructField('my_map', MapType(StringType(),
        StructType([
        StructField('age', DoubleType(), True),
        StructField('height', DoubleType(), True)])
                                      , True)])

# Making decisions
from pyspark.sql.functions import col, when
streaming_random = stream_df.select(
when(col("state") == "Virginia","Home")\
.when(col("state") == "West Virginia","Vacation")\
.when(col("state").isNull() ,"Nothing")\
.otherwise(col("state")\
.alias("relative_location"))

# unwanted columns
drop_columns = ["relative_location", "friend"]
update_dataframe.drop(*drop_columns)

# groups
from pyspark.sql.functions import rand,when
from pyspark.sql.functions import rand, when

df_random = df.withColumn('age', when(rand() > 0.2, 1).otherwise(0))

df_random.groupBy("state").agg(avg("age"))

df_random.groupBy("state").agg({"age": "mean"}


df_random.groupBy("state").agg(min("age"))

df_random.groupBy("state").agg(sum("age"))

# UDF

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, StringType


def addString(string):
    return string + "_" + "this_is_added"



lambda variable.addStringUDF = udf(lambda i: addString(i), StringType())

df_random.select(col("State"), \
                 addStringUDF(col("name")) \
                 .alias("name_and_more"))


spark.udf.register("addStringUDF", addString, StringType())

df_random.createOrReplaceTempView("my_test")
spark.sql("select state, addStringUDF(Name) as name_and_more from my_test")





@udf(returnType=StringType())
def addString(string):
    return string + "_" + "this_is_added"




from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, lit, struct


@udf(returnType=IntegerType())
def magic_calc(x):
    return x[0] * x[1]



for processing in our UDF.
df_random_2 = df_random.select(col("*"), lit(6).alias("height"))
df_random_2.select(col("State"), \
                   magic_calc(struct("age", "height")) \
                   .alias("name_and_more")).show()


from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import StringType


with pandas series objects.At first might seem like a huge difference but you are stepping outside the world of Spark.There is a significant speed increase with speed increase for some use-cases when compared to normal Python UDF’s.That being said we must keep in mind that the speed hit we take when compared to using normal Spark Dataframe APIs over using UDF’s.Just to reiterate UDF’s are edge cases where its impossible to accomplish something in normal Spark API and we are ok with the speed hit.


@pandas_udf('long')
def how_old(age: pd.Series) -> pd.Series:
    return age + 10


df_random.select(col("State"), \
                 how_old(col("age")) \
                 .alias("updated_age")).show()

# streaming

location = "dbfs:/tmp/parquet_fake_data"
fmt = "parquet"
df = spark.read.format(fmt).load(location)
stream_df = spark.readStream.schema(df.schema).format(fmt).load(location)

stream_df.isStreaming

from pyspark.sql.functions import col, when
streaming_random = stream_df.select(
when(col("state") == "Virginia","Home")
                                 .when(col("state") == "West Virginia","Vacation")
                                 .when(col("state").isNull() ,"Nothing")
                                 .otherwise(col("state").alias("relative_location"))
)


query = streaming_random.writeStream.format("console").outputMode("append").start()

query = streaming_random.writeStream.format("memory").queryName("test_table") .outputMode("append").start()

test = spark.table("test_table")

save_location = "..."
checkpoint_location = f"{save_location}/_checkpoint"
output_mode = "append"
streaming_random.writeStream.format("parquet").option("checkpointLocation", checkpoint_location).outputMode(output_mode).start()

streaming_random.writeStream.format("delta").option("checkpointLocation", checkpoint_location).trigger(once=True).outputMode(output_mode).start()



# LAB

# setup

%sh
rm -rf /dbfs/tmp/chapter_4_lab_test_data
rm -rf /dbfs/tmp/chapter_4_lab_bronze
rm -rf /dbfs/tmp/chapter_4_lab_silver
rm -rf /dbfs/tmp/chapter_4_lab_gold

fake = Faker()
def generate_data(num):
    row = [{"name":fake.name(),
           "address":fake.address(),
           "city":fake.city(),
           "state":fake.state(),
           "purchase_date":fake.date_time(),
            "purchase_id":fake.pyfloat(),
             "sales":fake.pyfloat()
           }]
    return row
panda = pd.DataFrame(generate_data(2))
fake_data = spark.createDataFrame(panda)
fake_data.write.format("parquet").mode("append").save("dbfs:/tmp/chapter_4_lab_test_data")

#1

location = "dbfs:/tmp/chapter_4_lab_test_data"
fmt = "parquet"
schema = spark.read.format(fmt).load(location).schema
users = spark.readStream.schema(schema).format(fmt).load(location)


bronze_schema = users.schema
bronze_location = "dbfs:/tmp/chapter_4_lab_bronze"
checkpoint_location = f"{bronze_location}/_checkpoint"
output_mode = "append"
bronze_query = users.writeStream.format("delta").trigger(once=True).option("checkpointLocation",
                                                                           bronze_location).option("path",
                                                                                                   bronze_location).outputMode(
    output_mode).start()
spark.read.format("delta").load(bronze_location).show()



from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, lit, struct, concat, col, abs, floor

bronze_location = "dbfs:/tmp/chapter_4_lab_bronze"
users_bronze = spark.readStream.format("delta").load(bronze_location)



@udf(returnType=StringType())
def strip_name(x):
    return x.split()[0]


address_columns = ["address", "city", "state"]
clean = users_bronze.select(col("*"), concat(*address_columns).alias("full_address"),
                            floor(abs("purchase_id")).alias("id"), strip_name("name").alias("first_name"))


silver_location = "dbfs:/tmp/chapter_4_lab_silver"
silver_checkpoint_location = f"{silver_location}/_checkpoint"
fmt = "delta"
output_mode = "append"
clean.writeStream.format("delta").option("checkpointLocation", silver_checkpoint_location).option("path",
                                                                                                  silver_location).trigger(
    once=True).outputMode(output_mode).start()
spark.read.format("delta").load(silver_location).show()
with the intent to do some aggregate calculations.
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit, struct, sum, avg, max, min

bronze_location = "dbfs:/tmp/chapter_4_lab_silver"
schema = spark.read.format(fmt).load(silver_location).schema
users_silver = spark.readStream.format("delta").load(silver_location)

gold = users_silver.groupBy("state").agg(min("sales").alias("min_sales"), max("sales").alias("max_sales"),
                                         avg("sales").alias("avg_sales"))
gold_location = "dbfs:/tmp/chapter_4_lab_gold"
gold_checkpoint_location = f"{silver_location}/_checkpoint"
fmt = "delta"
output_mode = "complete"

gold.writeStream.format("delta").option("checkpointLocation", gold_location).option("path", gold_location).trigger(
    once=True).outputMode(output_mode).start()
spark.read.format("delta").load(gold_location).show()



