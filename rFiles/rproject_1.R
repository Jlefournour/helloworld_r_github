library(sparklyr)


detach("package:rsparkling", unload = TRUE)
if ("package:h2o" %in% search()) { detach("package:h2o", unload = TRUE) }
if (isNamespaceLoaded("h2o")){ unloadNamespace("h2o") }
remove.packages("h2o")
install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-ueno/8/R")


spark_install(version = "2.2.0")

install.packages(c("nycflights13", "Lahman"))

library(sparklyr)
sc <- spark_connect(master = "local")

#Using dplyr
#We can now use all of the available 
#dplyr verbs against the tables 
#within the cluster.

#We’ll start by copying some datasets from R 
#into the Spark cluster 
#(note that you may need to install the 
#nycflights13 and Lahman packages in order to execute this code):

library(dplyr)
iris_tbl <- copy_to(sc, iris)
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")
src_tbls(sc)

# filter by departure delay and print the first few records
flights_tbl %>% filter(dep_delay == 2)

delay <- flights_tbl %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect


# plot delays
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)

#Using SQL
#It’s also possible to execute SQL queries 
#directly against tables within a Spark cluster. 
#The spark_connection object implements a DBI interface for Spark, 
#so you can use dbGetQuery to execute SQL and return the result as an R #data frame:

library(DBI)
iris_preview <- dbGetQuery(sc, "SELECT * FROM iris LIMIT 10")
iris_preview

# Machine Learning
# copy mtcars into spark
mtcars_tbl <- copy_to(sc, mtcars)

# transform our data set, and then partition into 'training', 'test'
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

# fit a linear model to the training dataset
fit <- partitions$training %>%
  ml_linear_regression(response = "mpg", features = c("wt", "cyl"))
## * No rows dropped by 'na.omit' call
fit
## Call: ml_linear_regression(., response = "mpg", features = c("wt", "cyl"))
## 
## Coefficients:
## (Intercept)          wt         cyl 
##   33.499452   -2.818463   -0.923187

summary(fit)

#Reading and Writing Data
#You can read and write data in CSV, JSON, and Parquet formats. 
#Data can be stored in HDFS, S3, or on the local filesystem of cluster nodes

temp_csv <- tempfile(fileext = ".csv")
temp_parquet <- tempfile(fileext = ".parquet")
temp_json <- tempfile(fileext = ".json")

spark_write_csv(iris_tbl, temp_csv)
iris_csv_tbl <- spark_read_csv(sc, "iris_csv", temp_csv)

spark_write_parquet(iris_tbl, temp_parquet)
iris_parquet_tbl <- spark_read_parquet(sc, "iris_parquet", temp_parquet)

spark_write_json(iris_tbl, temp_json)
iris_json_tbl <- spark_read_json(sc, "iris_json", temp_json)

src_tbls(sc)

#spark_apply deals with distributed data - apply r code to clusters of data
spark_apply(iris_tbl, function(data) {
  data[1:4] + rgamma(1,2)
})

#You can also group by columns to perform an operation 
#over each group of rows and make use of any package within the closure:
  
  spark_apply(
    iris_tbl,
    function(e) broom::tidy(lm(Petal_Width ~ Petal_Length, e)),
    names = c("term", "estimate", "std.error", "statistic", "p.value"),
    group_by = "Species"
  )
  
  #Extensions - using sparklyr to extend to third party libs
  #e.g. interfaces to custom machine learning pipelines, 
  #interfaces to 3rd party Spark packages, etc.
  
  # write a CSV 
  tempfile <- tempfile(fileext = ".csv")
  write.csv(nycflights13::flights, tempfile, row.names = FALSE, na = "")
  
  # define an R interface to Spark line counting
  count_lines <- function(sc, path) {
    spark_context(sc) %>% 
      invoke("textFile", path, 1L) %>% 
      invoke("count")
  }
  
  # call spark to count the lines of the CSV
  count_lines(sc, tempfile)
  
  
  #Table Utilities
  #You can cache a table into memory with:
  
  tbl_cache(sc, "batting")
  
  #and unload from memory using:
    
  tbl_uncache(sc, "batting")
  
  #Connection Utilities
  # You can view the Spark web console using the spark_web function:
  
  spark_web(sc)
  
  # You can show the log using the spark_log function:
  
  spark_log(sc, n = 10)
  
  
  #Using H2O
  #rsparkling is a CRAN package from H2O that extends 
  #sparklyr to provide an interface into Sparkling Water. 
  #For instance, the following example installs, configures and runs h2o.glm:
  
  options(rsparkling.sparklingwater.version = "2.2.0")
  library(rsparkling)
  library(sparklyr)
  library(dplyr)
  library(h2o)
  localH2O = h2o.init()
  sc <- spark_connect(master = "local", version = "2.2.0")
  mtcars_tbl <- copy_to(sc, mtcars, "mtcars")
  mtcars_h2o <- as_h2o_frame(sc, mtcars_tbl, strict_version_check = FALSE)
  

  #Finally, we disconnect from Spark:
  spark_disconnect(sc)
  
  
  
  
  