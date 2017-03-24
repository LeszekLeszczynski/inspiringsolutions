create database if not exists bzwbk;

use bzwbk;

-- target table

CREATE TABLE pos (
  karta_id int,  
  waluta string,                                               
  kwota double,                                                
  data_trans date,                                                 
  czas string,
  kraj string,
  kod string)
PARTITIONED BY (okres string)
STORED AS ORC;

-- interim table

CREATE EXTERNAL TABLE pos_in(
  karta_id int,    
  waluta string,                                             
  kwota double,                                                
  data_trans date,                                                 
  czas string,
  kraj string,
  kod string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/etl/manual/bzwbk/POS'
TBLPROPERTIES ("skip.header.line.count"="1");

-- load the data with "okres" extracted from filename

INSERT OVERWRITE TABLE pos PARTITION (okres)
  SELECT
    karta_id,                                                 
    waluta,
    kwota,                                                
    data_trans,                                                 
    czas,
    kraj,
    kod,
    regexp_extract(INPUT__FILE__NAME,'.*POS_(.*)\.TXT', 1) AS okres
  FROM pos_in;