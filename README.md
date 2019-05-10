# Big-Data
Cork Smart Gateway is the home of open data for Cork: http://data.corkcity.ie/

At the moment it contains 8 datasets in open format. 

One of them is the “Coca-Cola Zero Bikes”
(http://data.corkcity.ie/dataset/coca-cola-zero-bikes) which provides data related to the bikesharing rental service supported by the city council.
While historic information on the state of the 31 bike stations in Cork city is not available,
real-time information can be obtained via:

The Coca-Cola Zero Bikes website: Visual Format
https://www.bikeshare.ie/cork.html

The Open Data API Endpoint: JSON Format.
https://data.bikeshare.ie/dataapi/resources/station/data/list

{"schemeId":2, "schemeShortName":"Cork", "stationId":2032,
"name":"Kent Station", "nameIrish":"Stáisiún Ceannt", "docksCount":30,
"bikesAvailable":3, "docksAvailable":23, "status":0,
"latitude":51.902, "longitude": -8.458, "dateStatus":"15-10-2018 15:32:30" }


MY_DATASET

My former colleague Michael O’Keefe collected data from mid January 2017 to late
September 2017 by creating a service quering the API every 5 minutes from 6am to midnight
and gathering all entries of a day into a file.

I have selected the files for the period 01/02/2017 – 31/08/2017 and provided the entries in
the following csv format: status ; name ; longitude ; latitude ; dateStatus ; bikesAvailable ; docksAvailable
For example, the aforementioned entry for Kent Station would be represented as follows:
0;Kent Station;-8.45821512;51.90196195;15-10-2018 15:32:30;3;23

In total, the dataset for the selected period contains 1,339,200 entries for 43,200 API requests
over 200 days. It is provided in the folder “my_dataset” and has to be uploaded to the
Databricks FileSystem (DBFS) via the databricks command line interface described in
the lecture notes.
