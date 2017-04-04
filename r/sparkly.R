library(sparklyr)
library(dplyr)

# if R complains about Spark not installed:
# spark_install()

Sys.setenv(SPARK_HOME="/usr/hdp/current/spark-client")

#spark_disconnect(sc)

# connect to local cluster
sc <- spark_connect(master = "yarn-client")

# use database
invoke(hive_context(sc), "sql", "USE bzwbk")

# reload Spark view in RStudio to see tables

library(DBI)
dbGetQuery(sc, "select * from pos limit 10")

### MAPS
library(ggmap)
library(sp)
library(rgdal)
library(ggplot2)
library(raster)
library(ggthemes)
library(scales)

options(scipen=999)

klient_stats <- tbl(sc, 'klient') %>% 
  dplyr:: select(okres, kod_pocztowy, wiek, staz_klient, liczba_rach_k_kred, liczba_rach_biez) %>% 
  group_by(okres, kod_pocztowy) %>%
  summarise(count=n(), 
            sredni_wiek=mean(wiek), 
            sredni_staz=mean(staz_klient), 
            srednia_liczba_rach_k_kred = mean(liczba_rach_k_kred), 
            dystrybucja_rach_k_kred = mean(ifelse(liczba_rach_k_kred>0,1,0)), 
            srednia_liczba_rach_biez = mean(liczba_rach_biez),
            dystrybucja_rach_biez = mean(ifelse(liczba_rach_biez>0,1,0))
            ) %>% 
  collect()

klient_stats <- merge(klient_stats, PL_zip, by="kod_pocztowy")

klient_stats_woj <- klient_stats %>% group_by(okres, woj) %>% summarise(wiek=round(weighted.mean(sredni_wiek, count),1),
                                                                 staz=round(weighted.mean(sredni_staz, count)),
                                                                 dystr_rach_k_kred = round(weighted.mean(dystrybucja_rach_k_kred, count),4),
                                                                 dystr_rach_biez = round(weighted.mean(dystrybucja_rach_biez, count),4),
                                                                 count_all=sum(count)) %>%
  filter(woj!="")

klient_stats_woj$woj <- iconv(klient_stats_woj$woj, from="UTF-8", to="ASCII//TRANSLIT")

save(klient_stats_woj, file="shiny/klient_stats_woj.RData")


klient_stats_pow <- klient_stats %>% group_by(okres, powiat) %>% summarise(wiek=round(weighted.mean(sredni_wiek, count),1),
                                                                 staz=round(weighted.mean(sredni_staz, count)),
                                                                 dystr_rach_k_kred = round(weighted.mean(dystrybucja_rach_k_kred, count),4),
                                                                 dystr_rach_biez = round(weighted.mean(dystrybucja_rach_biez, count),4),
                                                                 count_all=sum(count)) %>%
  filter(powiat!="")

klient_stats_pow$powiat <- tolower(iconv(klient_stats_pow$powiat, from="UTF-8", to="ASCII//TRANSLIT"))

save(klient_stats_pow, file="shiny/klient_stats_pow.RData")

#############################
### TIME SERIES TRANSACTIONS
#############################
raport_transakcje <- tbl(sc, 'transakcje') %>% 
  dplyr:: select(data, kwota, waluta) %>% 
  group_by(data, waluta) %>%
  summarise(count=n(),
            total=sum(kwota),
            srednia_kwota=mean(kwota)) %>% dplyr:: arrange(data) %>% 
  collect()

raport_transakcje <- as.data.frame(raport_transakcje)

library(reshape2)
raport_transakcje_total <- dcast(raport_transakcje, data ~ waluta, value.var="total")

save(raport_transakcje_total, file="shiny/raport_transakcje_total.RData")
remove(raport_transakcje_total)
remove(raport_transakcje)


#############
## New map
#############

library(rgdal)
library(leaflet)



