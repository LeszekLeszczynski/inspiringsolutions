options( java.parameters = "-Xmx8g" )
library(rJava)
library(RJDBC)

cp = c("/usr/hdp/current/hive-client/lib/hive-jdbc.jar", 
       "/usr/hdp/current/hadoop-client/hadoop-common.jar")
.jinit(classpath=cp) 

drv <- JDBC("org.apache.hive.jdbc.HiveDriver",
            "/usr/hdp/current/hive-client/lib/hive-jdbc.jar",
            identifier.quote="`")

conn <- dbConnect(drv, "jdbc:hive2://master0.hackaton:10000/bzwbk", "hive", "hive")

#show_databases <- dbGetQuery(conn, "show databases")
#show_databases
tables <- dbGetQuery(conn, "select * from klient limit 10")
tables


