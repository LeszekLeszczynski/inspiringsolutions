	create database if not exists bzwbk;

	use bzwbk;

	-- target table

	CREATE TABLE klient (
	  	klient_id int,                                           
		stan_cyw string,                                         
		wyksztalcenie string,                                    
		liczba_osob_gosp int,                                    
		dochod double,                                           
		posiada_nieruchomosc int,                                
		typ_dochodu int,                                         
		wiek int,                                                
		staz_klient int,                                         
		kod_pocztowy string,                                     
		kraj string,                                             
		liczba_rach_k_kred int,                                  
		liczba_trans_k_kred int,                                 
		kwota_trans_k_kred double,                               
		liczba_k_deb int,                                        
		liczba_wyplat_k_deb_atm int,                             
		liczba_plat_k_deb int,                                   
		kwota_trans_k_deb double,                                
		liczba_rach_biez int,                                    
		liczba_trans_rach_biez int,                              
		kwota_trans_rach_biez double,                            
		liczba_prod_kred int,                                    
		liczba_prod_oszcz int)
	PARTITIONED BY (okres string)
	STORED AS ORC;

	-- interim table

	CREATE EXTERNAL TABLE klient_in(
	  	klient_id int,                                           
		stan_cyw string,                                         
		wyksztalcenie string,                                    
		liczba_osob_gosp int,                                    
		dochod double,                                           
		posiada_nieruchomosc int,                                
		typ_dochodu int,                                         
		wiek int,                                                
		staz_klient int,                                         
		kod_pocztowy string,                                     
		kraj string,                                             
		liczba_rach_k_kred int,                                  
		liczba_trans_k_kred int,                                 
		kwota_trans_k_kred double,                               
		liczba_k_deb int,                                        
		liczba_wyplat_k_deb_atm int,                             
		liczba_plat_k_deb int,                                   
		kwota_trans_k_deb double,                                
		liczba_rach_biez int,                                    
		liczba_trans_rach_biez int,                              
		kwota_trans_rach_biez double,                            
		liczba_prod_kred int,                                    
		liczba_prod_oszcz int)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '|'
	STORED AS TEXTFILE
	LOCATION '/etl/manual/bzwbk/KLIENT'
	TBLPROPERTIES ("skip.header.line.count"="1");

	-- load the data with "okres" extracted from filename

	INSERT OVERWRITE TABLE klient PARTITION (okres)
	  SELECT
	    klient_id,                                     
		stan_cyw,                                      
		wyksztalcenie,                                    
		liczba_osob_gosp,                                    
		dochod,                                           
		posiada_nieruchomosc,                                
		typ_dochodu,                                         
		wiek,                                                
		staz_klient,                                         
		kod_pocztowy,                                     
		kraj,                                             
		liczba_rach_k_kred,                                  
		liczba_trans_k_kred,                                 
		kwota_trans_k_kred,                               
		liczba_k_deb,                                        
		liczba_wyplat_k_deb_atm,                             
		liczba_plat_k_deb,                                   
		kwota_trans_k_deb,                                
		liczba_rach_biez,                                    
		liczba_trans_rach_biez,                              
		kwota_trans_rach_biez,                            
		liczba_prod_kred,                                    
		liczba_prod_oszcz,
	    regexp_extract(INPUT__FILE__NAME,'.*KLIENT_(.*)\.TXT', 1) AS okres
	  FROM klient_in;