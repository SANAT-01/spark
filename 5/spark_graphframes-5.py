from pyspark.sql import SQLContext
from pyspark.sql.types import StructField,StringType
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

srt = datetime.now()

log_txt = sc.textFile("/user/sanattudu/roadNet-CA.txt")
sqlContext = SQLContext(sc)
from pyspark.sql.types import StructField,StringType

header = log_txt.first()
fields = [StructField(field_name, StringType(), True)
      for field_name in header.split(',')]
log_txt = log_txt.filter(lambda line: line != header) # Remove header from the txt file
temp_var = log_txt.map(lambda k: k.split("\t"))
      
df = sqlContext.createDataFrame(temp_var)

df = df.withColumn("value", lit(1))       
a =df.withColumnRenamed('_1','src')
b  =a.withColumnRenamed('_2','dst')
df  = b.withColumnRenamed('value','relationship')


d = [i for i in range(0,1965206)]

vertices = spark.createDataFrame(d,IntegerType())
d = vertices.withColumnRenamed('value','id')

from graphframes import GraphFrame
g = GraphFrame(d,df)

results = g.triangleCount()

ed = datetime.now()
print("start =", srt,"end = ",ed)

## no.of triangles from each node
results.show()