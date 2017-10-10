# Create a connection to the database
install.packages('RPostgreSQL')
library('RPostgreSQL')
library(dplyr)

## Option 2: Loading required package: DBI using dbplyr

#con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), 
                      #host = "localhost",
                     # user = "hadley",
                      #password = rstudioapi::askForPassword("Database password")
#)

#-----------------------------------------------------------------------------------
## Option 1: Loading required package: DBI

pg = dbDriver("PostgreSQL")

# Local Postgres.app database; no password by default
# Of course, you fill in your own database information here.

con = dbConnect(pg, user="power_user", password="tp3dotcom",
                host="localhost", port=5432, dbname="performance")


#--------------------------------------------------------------------------------------
stark_meter_data = as.data.frame(stark_meter_data)
summary(stark_meter_data)

# make names db safe: no '.' or other illegal characters,
# all lower case and unique
dbSafeNames = function(names) {
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

colnames(stark_meter_data) = dbSafeNames(colnames(stark_meter_data))
summary(stark_meter_data)

# write the table into the database.
# use row.names=FALSE to prevent the query 
# from adding the column 'row.names' to the table 
# in the db
dbWriteTable(con,'stark_meter_data',stark_meter_data, row.names=FALSE)

# read back the full table: method 1
dtab = dbGetQuery(con, "select * from stark_meter_data")
summary(dtab)

# read back the full table: method 2
rm(dtab)
dtab = dbReadTable(con, "stark_meter_data")
summary(dtab)

# get part of the table
rm(dtab)
dtab = dbGetQuery(con, "select meter_name from stark_meter_data")
summary(dtab)
rm(dtab)

dtab <- tbl(con, "stark_meter_data")
dtab

# remove table from database
#dbSendQuery(con, "drop table stark_meter_data")

# commit the change
#dbCommit(con)

# disconnect from the database
dbDisconnect(con)




