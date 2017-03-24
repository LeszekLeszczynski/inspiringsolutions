create database if not exists bzwbk;

use bzwbk;

-- target table

CREATE TABLE transakcje (
  rach_id int,
  waluta string,
  kwota double,
  data date,
  strona string,
  odbiorca string)
PARTITIONED BY (okres string)
STORED AS ORC;

-- interim table

CREATE EXTERNAL TABLE transakcje_in(
  rach_id int,
  waluta string,
  kwota double,
  data date,
  strona string,
  odbiorca string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/etl/manual/bzwbk/TRANSAKCJE'
TBLPROPERTIES ("skip.header.line.count"="1");

-- load the data with "okres" extracted from filename

INSERT OVERWRITE TABLE transakcje PARTITION (okres)
  SELECT
    rach_id,
    waluta,
    kwota,
    data,
    strona,
    odbiorca,
    regexp_extract(INPUT__FILE__NAME,'.*TRANSAKCJE_(.*)\.TXT', 1) AS okres
  FROM transakcje_in;