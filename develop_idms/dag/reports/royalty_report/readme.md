# Standardization of Royalty Reports

#
Fetch queries from sq server should be added to config file with format : "database_fetch"
By default the job will run weekly on Sunday and daily for all week days.

#
DAG runs in below modes:
'D' : Daily
'W' : Weekly
'DR' : Date Range

Just trigger the dag for daily/weekly run w/o any config and to get Royalty Reports for past dates, please run the dag with below config:
#example :

#for date range royalty reports and specific db's
{"run-info": {"mode": "DR", "start-date": "2021.05.10", "end-date": "2021.11.14", "db-list": "1267,992,1105"}
}

#Pre-requites
Count tables and respective tblmain for respective order id's should be present in redshift to get royalty.

#Config File:
1. database_id - List of database separated by ',' for which royalty is to be calculated
2. database_fetch - query to fetch data from sq server

#Deployment
dag name : 'reports-royalty-report-do-not-run-manually'
