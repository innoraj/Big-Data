import pyspark

'''
1Q.) Number of times each station ran out of bikes

Ans.)

'''
inputRDD=sc.textFile("/FileStore/tables")
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], userRecord[2],userRecord[3],userRecord[4],int(userRecord[5]),userRecord[6]])
data1 = data.filter(lambda x: (x[5] == 0 ))	 
res = data1.map(lambda x:(x[1],1)).reduceByKey( lambda accum, n: accum + n).sortBy(lambda x: x[0], ascending = False)
res.collect()





'''
2Q.) Number of times each station ran out of bikes
Ans.)
'''

inputRDD=sc.textFile("/FileStore/tables")
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], userRecord[2],userRecord[3],userRecord[4],int(userRecord[5]),userRecord[6]])
data1 = data.filter(lambda x: ("28-08-2017" in x[4]))
data2 = data1.map(lambda x:(x[1],x[5])).reduceByKey(lambda accum, n : accum + n).sortBy(lambda x: x[0], ascending = False)
data2.collect()





'''
3 Q.) with first one filtering
     Average amount of bikes per station and hour window
     (e.g. [9am, 10am), [10am, 11am), etc. )  :=> how many bikes where there per window
Ans.)
'''

import re
inputRDD=sc.textFile("/FileStore/tables")
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], userRecord[2],userRecord[3],userRecord[4],int(userRecord[5]),userRecord[6]])
data1 = data.filter(lambda x: (x[5] == 0 and "28-08-2017" in x[4]))
six_to_seven = re.compile("28-08-2017 (06):\\d{2}:\\d{2}")
seven_to_eight = re.compile("28-08-2017 (07):\\d{2}:\\d{2}")
eight_to_nine = re.compile("28-08-2017 (08):\\d{2}:\\d{2}")
nine_to_ten = re.compile("28-08-2017 (09):\\d{2}:\\d{2}")
ten_to_eleven = re.compile("28-08-2017 (10):\\d{2}:\\d{2}")
eleven_to_tweleve = re.compile("28-08-2017 (11):\\d{2}:\\d{2}")
tweleve_to_thirteen = re.compile("28-08-2017 (12):\\d{2}:\\d{2}")
thirteen_to_Fourteen = re.compile("28-08-2017 (13):\\d{2}:\\d{2}")
Fourteen_to_Fifteen = re.compile("28-08-2017 (14):\\d{2}:\\d{2}")
Fifteen_to_Sixteen = re.compile("28-08-2017 (15):\\d{2}:\\d{2}")
Sixteen_to_Seventy = re.compile("28-08-2017 (16):\\d{2}:\\d{2}")
Seventy_to_Eighty = re.compile("28-08-2017 (17):\\d{2}:\\d{2}")
Eighty_to_Ninteen = re.compile("28-08-2017 (18):\\d{2}:\\d{2}")
Ninteen_to_Tweenty = re.compile("28-08-2017 (19):\\d{2}:\\d{2}")
Tweenty_to_Tweentyone = re.compile("28-08-2017 (20):\\d{2}:\\d{2}")
Tweentyone_to_Tweentytwo = re.compile("28-08-2017 (21):\\d{2}:\\d{2}")
Tweentytwo_to_Tweentythree = re.compile("28-08-2017 (22):\\d{2}:\\d{2}")
Tweentythree_to_TweentyFourth = re.compile("28-08-2017 (23):\\d{2}:\\d{2}")

six_to_seven_Count = data1.filter(lambda x: six_to_seven.match(x[4]) != None)
seven_to_eight_Count = data1.filter(lambda x: seven_to_eight.match(x[4]) != None)
eight_to_nine_Count = data1.filter(lambda x: eight_to_nine.match(x[4]) != None)
nine_to_ten_Count = data1.filter(lambda x: nine_to_ten.match(x[4]) != None)
ten_to_eleven_Count = data1.filter(lambda x: ten_to_eleven.match(x[4]) != None)
eleven_to_tweleve_Count = data1.filter(lambda x: eleven_to_tweleve.match(x[4]) != None)
tweleve_to_thirteen_Count = data1.filter(lambda x: tweleve_to_thirteen.match(x[4]) != None)
thirteen_to_Fourteen_Count = data1.filter(lambda x: thirteen_to_Fourteen.match(x[4]) != None)
Fourteen_to_Fifteen_Count = data1.filter(lambda x: Fourteen_to_Fifteen.match(x[4]) != None)
Fifteen_to_Sixteen_Count = data1.filter(lambda x: Fifteen_to_Sixteen.match(x[4]) != None)
Sixteen_to_Seventy_Count = data1.filter(lambda x: Sixteen_to_Seventy.match(x[4]) != None)
Seventy_to_Eighty_Count = data1.filter(lambda x: Seventy_to_Eighty.match(x[4]) != None)
Eighty_to_Ninteen_Count = data1.filter(lambda x: Eighty_to_Ninteen.match(x[4]) != None)
Ninteen_to_Tweenty_Count = data1.filter(lambda x: Ninteen_to_Tweenty.match(x[4]) != None)
Tweenty_to_Tweentyone_Count = data1.filter(lambda x: Tweenty_to_Tweentyone.match(x[4]) != None)
Tweentyone_to_Tweentytwo_Count = data1.filter(lambda x: Tweentyone_to_Tweentytwo.match(x[4]) != None)
Tweentytwo_to_Tweentythree_Count = data1.filter(lambda x: Tweentytwo_to_Tweentythree.match(x[4]) != None)
Tweentythree_to_TweentyFourth_Count = data1.filter(lambda x: Tweentythree_to_TweentyFourth.match(x[4]) != None)

six_to_seven_Count.count() 
seven_to_eight_Count.count()
eight_to_nine_Count.count() 
nine_to_ten_Count.count() 
ten_to_eleven_Count.count() 
eleven_to_tweleve_Count.count() 
tweleve_to_thirteen_Count.count() 
thirteen_to_Fourteen_Count.count() 
Fourteen_to_Fifteen_Count.count() 
Fifteen_to_Sixteen_Count.count() 
Sixteen_to_Seventy_Count.count() 
Seventy_to_Eighty_Count.count() 
Eighty_to_Ninteen_Count.count() 
Ninteen_to_Tweenty_Count.count()
Tweenty_to_Tweentyone_Count.count() 
Tweentyone_to_Tweentytwo_Count.count() 
Tweentytwo_to_Tweentythree_Count.count() 
Tweentythree_to_TweentyFourth_Count.count()





'''
Exercise 4: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017  Get the different ran-outs to attend.  Note: n consecutive measurements of a station being ran-out of bikes has to be considered a single ran-out, that should have been attended when the ran-out happened in the first time. 
'''


'''
5Q) Sunday 28th August 2017
 Get the station with biggest number of bikes for each ran-out to be attended. :=> ( highest number of 0 occured for station)
Ans.)''' 	 
	 
inputRDD=sc.textFile("/FileStore/tables")
data = inputRDD.map(lambda line: line.split(";")).map(lambda userRecord: [userRecord[0],userRecord[1], userRecord[2],userRecord[3],userRecord[4],int(userRecord[5]),userRecord[6]])
data1 = data.filter(lambda x: (x[5] == 0 and "28-08-2017" in x[4]))	 
res = data1.map(lambda x:(x[1],1)).reduceByKey( lambda accum, n: accum + n)
res.takeOrdered(1, key = lambda x: -x[1])