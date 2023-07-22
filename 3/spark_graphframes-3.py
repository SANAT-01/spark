from pyspark.sql import SQLContext
from pyspark.sql.types import StructField,StringType
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
import random
import time
from pyspark.sql.functions import udf,col
from graphframes import GraphFrame
from pyspark.sql.types import DoubleType

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName('Practise').getOrCreate()

log_txt = sc.textFile("/user/venu/roadNet_CA.txt")
sqlContext = SQLContext(sc)

header = log_txt.first()
fields = [StructField(field_name, StringType(), True)
      for field_name in header.split(',')]
log_txt = log_txt.filter(lambda line: line != header) # Remove header from the txt file
temp_var = log_txt.map(lambda k: k.split("\t"))
      
df = sqlContext.createDataFrame(temp_var)

df = df.withColumn("value", lit(1))       
a =df.withColumnRenamed('_1','src')
b =a.withColumnRenamed('_2','dst')
df  = b.withColumnRenamed('value','relationship')

d = random.sample(range(0,1965207),1000)
dff = spark.createDataFrame([("","")], ['avgdist','time'])

def aa(dist):
    dist = dist.values()
    avg = sum(dist)/21
    return avg
for i in d:    
    vertices = spark.createDataFrame([i],IntegerType())
    a = vertices.withColumnRenamed('value','id')
    
    g = GraphFrame(a,df)
    e = [j for j in range(0,1965207)]
    
    start = time.time()
    results = g.shortestPaths(landmarks=e)
    udf_star_desc = udf(lambda x:aa(x),StringType()) 

    
    end= time.time()
    newRow = spark.createDataFrame([(results.withColumn("avg",udf_star_desc(col("distances"))).collect()[0][2],end-start)], ['avgdist','time'])
    dff = dff.union(newRow)
# df.show()  gives dataframe contains avg distance and time taken


dff = dff.withColumn("avgdist", dff["avgdist"].cast(DoubleType()).alias("avgdist"))
dff = dff.withColumn("time", dff["time"].cast(DoubleType()).alias("time"))
# dff.printSchema()  

#Scatter plot
import matplotlib.pyplot as plt
from pyspark.sql import SQLContext
dff.registerTempTable('df')
df3 = sqlContext.sql("SELECT avgdist,time from df ").toPandas()
fig=plt.figure()
ax=fig.add_axes([0,0,1,1])
ax.scatter(df3['time'],df3['avgdist'], color='r')
plt.show()


# correlation between avgdist and time
from pyspark.ml.stat import Correlation
dff.corr('avgdist','time')





