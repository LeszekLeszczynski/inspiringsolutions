################################
## Getting hive tables into R ##
################################

## Load required libraries
library(sparklyr)

## Set the directory with Spark
Sys.setenv(SPARK_HOME="/usr/hdp/current/spark-client")

# Connect to the local cluster
sc <- spark_connect(master = "yarn-client")

# On the right there should be a "Spark" tab next to "Environment" and "History"

# Use the database - get the HiveContext associated with a connection 
invoke(hive_context(sc), "sql", "USE bzwbk")

##### IMPORTANT ##############################################################################
## In order to see available tables and variables they contain you should reload the Spark tab 
## (right-hand upper side of RStudio) using the arrow "Refresh data"
##############################################################################################

##############
## METHOD 1 ##
##############

## The easiest way to work with the data: Use dplyr with Spark
library(dplyr)

## With that library you can:
## - select() needed columns
## - mutate() the table and add calculated columns
## - filter() it
## - group_by() multiple variables
## - summarise() them by calculating aggregate statistics

## The most convinient way is to create processing pipelines using "%>%" symbol
## It transfers the result of a calculation to the next calculation
## For example, if "airline" is the data frame we want to process:
## result <- airline %>% dplyr:: select(date, flight_delay) %>% group_by(date) %>% summarise(cnt = n(), avg=mean(flight_delay)
## It will calculate the total number of flights per day and the average delay time
## ** with select() always use dplyr:: select() - otherwise it will conflict with ...
## ...the select() from MASS package

## Link to the tutorial:
## https://cran.r-project.org/web/packages/dplyr/vignettes/introduction.html

## Example: calculate basic statistics for klient table and save it in the 
## environment as "example" data.frame
example1 <- tbl(sc, 'klient') %>% 
  dplyr:: select(kod_pocztowy, wiek, staz_klient, liczba_rach_k_kred, liczba_rach_biez) %>% 
  group_by(kod_pocztowy) %>% 
  summarise(count=n(), 
            sredni_wiek=mean(wiek), 
            sredni_staz=mean(staz_klient), 
            srednia_liczba_rach_k_kred = mean(liczba_rach_k_kred), 
            dystrybucja_rach_k_kred = mean(ifelse(liczba_rach_k_kred>0,1,0)), 
            srednia_liczba_rach_biez = mean(liczba_rach_biez),
            dystrybucja_rach_biez = mean(ifelse(liczba_rach_biez>0,1,0))
  ) %>% 
  collect()

##############
## METHOD 2 ##
##############

## Use SQL to create needed data.frames

## Load needed libraries
library(DBI)

## Examples
example2 <- dbGetQuery(sc, "select * from klient where wyksztalcenie='S' limit 10")
example3 <- dbGetQuery(sc, "select waluta, count(kwota) as total from pos where okres='1612' group by waluta limit 10")

