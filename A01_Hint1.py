import pyspark

# 1. We load the dataset into an inputRDD
inputRDD=sc.textFile("/FileStore/tables")
# 2. We count the total amount of entries
totalcount = inputRDD.count()
# 3. We print the result
print(totalcount)
# totalcount.show()




# Exercise 2: Number of Coca-cola bikes stations in Cork.

inputRDD=sc.textFile("/FileStore/tables")
#inputRDD.count()
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], float(userRecord[2]),float(userRecord[3]),userRecord[4],int(userRecord[5]),userRecord[6]])
data.count()
#data.filter(lambda x: ("Cork" in x[1])).collect()  # U will get List
#data.filter(lambda x: "Cork" in x[1]).count()     # U will get Count
#data.filter(lambda x: "Cork" in x[1]).distinct().count()     # U will get Count as Distinct




#Exercise 3: List of Coca-Cola bike stations.

inputRDD=sc.textFile("/FileStore/tables")
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], float(userRecord[2]),float(userRecord[3]),userRecord[4],int(userRecord[5]),userRecord[6]])
data.count()
#data.map( lambda x:x[1]).distinct().count()  # it is For Distinct Count
#data.map( lambda x:x[1]).distinct().collect() # it is Give as List




# Exercise 4: Sort the bike stations by their longitude (East to West). 

inputRDD=sc.textFile("/FileStore/tables")
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], float(userRecord[2]),float(userRecord[3]),userRecord[4],int(userRecord[5]),userRecord[6]])
data.sortBy(lambda x: x[2]).map(lambda x:[x[1],x[2]]).collect()




# Exercise 5: Average number of bikes available at Kent Station.

inputRDD=sc.textFile("/FileStore/tables")
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], float(userRecord[2]),float(userRecord[3]),userRecord[4],int(userRecord[5]),userRecord[6]])