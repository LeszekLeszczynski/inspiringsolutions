create database if not exists bzwbk;

use bzwbk;

-- target table

CREATE TABLE karty (
  klient_id int,
  karta_id int,
  typ string,
  data_otw date,
  limit double)
PARTITIONED BY (okres string)
STORED AS ORC;

-- interim table

CREATE EXTERNAL TABLE karty_in(
  klient_id int,
  karta_id int,
  typ string,
  data_otw date,
  limit double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/etl/manual/bzwbk/KARTY'
TBLPROPERTIES ("skip.header.line.count"="1");

-- load the data with "okres" extracted from filename

INSERT OVERWRITE TABLE karty PARTITION (okres)
  SELECT
    klient_id,
    karta_id,
    typ,
    data_otw,
    limit,
    regexp_extract(INPUT__FILE__NAME,'.*KARTY_(.*)\.TXT', 1) AS okres
  FROM karty_in;