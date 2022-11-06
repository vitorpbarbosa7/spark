from pyspark.sql import SparkSession
from pyspark.sql import Row

# Instead of Spark Context, for Spark SQL we create a Spark Session
# It's like opening a database session, so we must close it at the end
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    '''
    Mapper to read from file which does not have headers, 
    so we have to explicitly build the structure
    '''

    fields = line.split(',')

    return Row(
        ID=int(fields[0]),
        name=str(fields[1]),
        age=int(fields[2]),
        numFriends=int(fields[3])
        )

lines = spark.sparkContext.textFile('fakefriends.csv')
people = lines.map(mapper)

# Spark SQL
# cache to keep it in memory
# caches the specified DataFrame, Dataset, or RDD in the memory of your cluster's workers.
schemaPeople = spark.createDataFrame(people).cache()

# To be able to query it with spark sql, must create a Temporary View with the following 
# command giving it a name
# That way you register a dataframe as a table for spark sql execute the operations
schemaPeople.createOrReplaceTempView("people")

schemaPeople.printSchema()

query = f'''
SELECT 
*
FROM
people
WHERE
age >= 13 
AND
age <= 19
'''
teenagers = spark.sql(query)

for teen in teenagers.collect():
    print(teen)

# Using functions instead of spark sql
schemaPeople.filter((schemaPeople['age'] >= 13) & (schemaPeople['age'] <= 19)).show()

# Close connection
spark.stop()

