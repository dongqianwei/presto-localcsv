# presto-localcsv
a presto plugin supporting read csv files in local filesystem.

## connector config file: localcsv.properties
```
connector.name=localcsv

csv.root=/home/name/csvdb
```

csv.root sets the root dir of the data files, schema name is the second level directory,
table name is the csv file name without suffix.

for example:
in connector config file:
```
csv.root = /var/data
```
if you run sql query:
```
select * from localcsv.default.table;
```
then the plugin will load content from /var/data/default/table.csv
