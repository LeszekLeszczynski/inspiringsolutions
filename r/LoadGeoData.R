#########################################################################
#### A script which loads the geospatial data for map visualizations ####
#########################################################################

#################################################################################
## Load data with zipcodes for Poland
temp <- tempfile()
download.file("http://download.geonames.org/export/zip/PL.zip",temp)
PL_zip <- read.csv2(unz(temp, "PL.txt"), sep="\t", header=FALSE)
unlink(temp)
PL_zip <- PL_zip %>% dplyr:: select(2,3,4,6,10,11)
names(PL_zip) <- c("kod_pocztowy", "miasto", "woj", "powiat", "szer_geo", "dl_geo")
PL_zip$woj <- tolower(PL$woj)
PL_zip <- PL_zip %>% dplyr:: select(1,2,3,4)

## Save results
save(PL_zip, file="PL_zip.RData")
save(PL_zip, file="shiny/PL_zip.RData")
#################################################################################

#################################################################################
## Load SpatialPolygons data
## WOJEWODZTWA
wojs <- getData('GADM', country='POL', level=1)
wojs@data$woj <- tolower(iconv(wojs@data$VARNAME_1, from="UTF-8", to="ASCII//TRANSLIT"))
wojs@data$VARNAME_1 <- tolower(wojs@data$VARNAME_1)
wojs@data$VARNAME_1[wojs@data$VARNAME_1=="Lódzkie"] <- "łódzkie"
wojs@data$VARNAME_1[wojs@data$VARNAME_1=="swietokrzyskie"] <- "świętokrzyskie"
wojs@data$VARNAME_1[wojs@data$VARNAME_1=="malopolskie"] <- "małopolskie"
wojs@data$VARNAME_1[wojs@data$VARNAME_1=="dolnoslaskie"] <- "dolnośląskie"
wojs@data$VARNAME_1[wojs@data$VARNAME_1=="slaskie"] <- "śląskie"
wojs@data$VARNAME_1[wojs@data$VARNAME_1=="warminsko-mazurskie"] <- "warmińsko-mazurskie"


## POWIATY
powiaty <- getData('GADM', country='POL', level=2)

powiaty@data$NAME_2[powiaty@data$NAME_2=="Warsaw"] <- "Warszawa"
powiaty@data$NAME_2[powiaty@data$NAME_2=="Ople"] <- "Opole"
powiaty@data$NAME_2[powiaty@data$NAME_2=="Zielona"] <- "Zielona Góra"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="Powiat Lipski|Lipski"] <- "Powiat lipski"
powiaty@data$powiat <- gsub("Powiat ", "", powiaty@data$VARNAME_2)
powiaty@data$VARNAME_2 <- tolower(powiaty@data$VARNAME_2)
powiaty@data$VARNAME_2[powiaty@data$ENGTYPE_2=="City"] <- gsub(" City","",powiaty@data$NAME_2[powiaty@data$ENGTYPE_2=="City"])
powiaty@data$powiat[powiaty@data$ENGTYPE_2=="City"] <- gsub(" City","",powiaty@data$NAME_2[powiaty@data$ENGTYPE_2=="City"])
powiaty@data$powiat <- tolower(iconv(powiaty@data$powiat, from="UTF-8", to="ASCII//TRANSLIT"))
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat lódzki wschodni"] <- "powiat łódzki wschodni"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat lomżyński"] <- "powiat łomżyński"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat losicki"] <- "powiat łosicki"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat lowicki"] <- "powiat łowicki"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat lęczyński"] <- "powiat łęczyński"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat lukowski"] <- "powiat łukowski"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat sremski"] <- "powiat śremski"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat sredzki"] <- "powiat średzki"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat swiecki"] <- "powiat świecki"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat zniński"] <- "powiat żniński"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat zagański"] <- "powiat żagański"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat swidwiński"] <- "powiat świdwiński"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat swidnicki"] <- "powiat świdnicki"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat lobeski"] <- "powiat łobeski"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat zywiecki"] <- "powiat żywiecki"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat zyrardowski"] <- "powiat żyrardowski"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat zuromiński"] <- "powiat żuromiński"
powiaty@data$VARNAME_2[powiaty@data$VARNAME_2=="powiat zarski"] <- "powiat żarski"


## Save results
save(wojs, file="shiny/wojs.RData")
save(powiaty, file="shiny/powiaty.RData")
#################################################################################
