create database if not exists bzwbk;

use bzwbk;

-- target table

CREATE TABLE rachunek(
  rach_id int,
  klient_id int,
  iban string,
  saldo double,
  data_otw date,
  data_ost_trans date,
  waluta string,
  produkt string, 
  obrot double,
  limit double)
PARTITIONED BY (okres string)
STORED AS ORC;

-- interim table

CREATE EXTERNAL TABLE rachunek_in(
  rach_id int,
  klient_id int,
  iban string,
  saldo double,
  data_otw date,
  data_ost_trans date,
  waluta string,
  produkt string,
  obrot double,
  limit double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/etl/manual/bzwbk/RACHUNEK'
TBLPROPERTIES ("skip.header.line.count"="1");

-- load the data with "okres" extracted from filename

INSERT OVERWRITE TABLE rachunek PARTITION (okres)
  SELECT
    rach_id,
    klient_id,
    iban,
    saldo,
    data_otw,
    data_ost_trans,
    waluta,
    produkt,
    obrot,
    limit,
    regexp_extract(INPUT__FILE__NAME,'.*RACHUNEK_(.*)\.TXT', 1) AS okres
  FROM rachunek_in;