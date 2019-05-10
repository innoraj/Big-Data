import pyspark

inputRDD=sc.textFile("/FileStore/tables")

dataRDD = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], float(userRecord[2]),float(userRecord[3]),userRecord[4],int(userRecord[5]),userRecord[6]])

#df = spark.createDataFrame(dataRDD).toDF("a", "b","c","d","e","f","g")
datadf  = spark.createDataFrame(dataRDD).toDF("status", "name","longitude","latitude","dateStatus","bikesAvailable","docksAvailable")

datadf.createOrReplaceTempView("test")
datadf.show()

filter_ZeroDf = spark.sql("select * from test where bikesAvailable = 0")

filter_ZeroDf.createOrReplaceTempView("testZero")


'''
1.) Exercise 1: Number of times each station ran out of bikes (sorted decreasingly by station). 

Ans:=>
'''

result_df = spark.sql("select name,count(bikesAvailable) from testZero group by name,bikesAvailable order by name desc")
 
result_df.show()
 
 
'''
2.) Exercise 2: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017  Total amount of bikes availables being measured per station  (sorted decreasingly by number of bikes) 

''' 
 
get_Datadf = spark.sql("select * from testZero where dateStatus like '28-08-2017%'")

get_Datadf.createOrReplaceTempView("datetable")

df_res = spark.sql("select name,count(bikesAvailable) from datetable group by name,bikesAvailable order by name asc")

df_res.show() 

result_df .show() 




'''
5Q) Sunday 28th August 2017
	 Get the station with biggest number of bikes for each ran-out to be attended. :=> ( highest number of 0 occured for station)
Ans.) 

'''	

get_Datadf = spark.sql("select * from testZero where dateStatus like '28-08-2017%'")
	 
df_res = spark.sql("select name,count(bikesAvailable) from datetable group by name,bikesAvailable order by name asc limit 1")
	 
df_res.show()