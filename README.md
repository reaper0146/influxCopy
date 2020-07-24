# influxCopy
Copy data from one influxBD to another

1. Install required python packages
2. Run code as ./influxCopy.py rIP rUser rPass rDB  LIP LUser LPass LDB
   where r refers to details of database from which data is to be copied and L refers to details of database to which data is to be copied
3. The start and end time can be specified on lines 18 and 19 respectively

Copies data 5 minutes at a time
