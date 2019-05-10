import pyspark

inputRDD=sc.textFile("/FileStore/tables")
dataRDD = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], float(userRecord[2]),float(userRecord[3]),userRecord[4],int(userRecord[5]),userRecord[6]])
df = spark.createDataFrame(dataRDD).toDF("status", "name","longitude","latitude","dateStatus","bikesAvailable","docksAvailable")
df.createOrReplaceTempView("testtable")
df.show()





# Exercise 1: Total amount of entries in the dataset.
count_data = spark.sql("select count(*) from testtable")
count_data.show()





# Exercise 2: Number of Coca-cola bikes stations in Cork.

number_CocaStation = spark.sql("select * from testtable where name like  'Cork%'")
number_CocaStation.show()





# Exercise 3: List of Coca-Cola bike stations.

bike_StationCount = spark.sql("select dinamestinct() from testtable")
bike_StationCount.show(20)  # For Showing 20 Record
bike_StationCount.count()  # For Count





# Exercise 4: Sort the bike stations by their longitude (East to West). 

sort_bikeStation = spark.sql("select name,longitude from testtable order by longitude ")
sort_bikeStation.distinct().show()





# Exercise 5: Average number of bikes available at Kent Station.

number_CocaStation = spark.sql("select avg(bikesAvailable) as average from testtable where name like  'Kent Station%'")
number_CocaStation.show()