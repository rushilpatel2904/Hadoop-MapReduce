REGISTER file:/home/hadoop/lib/pig/piggybank.jar 
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader; 


-- Set defaul reduce tasts to 10
SET default_parallel 10;

-- Get all data for Flight 1 [ Left Leg ]
F1 = LOAD '$INPUT' USING CSVLoader(',');
F1 = FOREACH F1 GENERATE  	(int)$0 AS (year1),
							(int)$2 AS (month1),
							(chararray)$5 AS (flightDate1),
							(chararray)$11 AS (origin1),
							(chararray)$17 AS (destination1),
							(int)$24 AS (departureTime1),
							(int)$35 AS (arrivalTime1),
							(int)$37 AS (arrivalDelayMins1),
							(int)$41 AS (cancelled1),
							(int)$43 AS (diverted1);

-- Get all data for Flight 2 [ Right Leg ]
F2 = LOAD '$INPUT' USING CSVLoader(',');					
F2 = FOREACH F2 GENERATE  	(int)$0 AS (year2),
							(int)$2 AS (month2),
							(chararray)$5 AS (flightDate2),
							(chararray)$11 AS (origin2),
							(chararray)$17 AS (destination2),
							(int)$24 AS (departureTime2),
							(int)$35 AS (arrivalTime2),
							(int)$37 AS (arrivalDelayMins2),
							(int)$41 AS (cancelled2),
							(int)$43 AS (diverted2);

-- Fliter Flights 1 and 2 according to destination, origin, cancelled and diverted values including checks for valid flightdates.
F1 = FILTER F1 BY (cancelled1 !=1) AND (diverted1 !=1) AND (origin1 == 'ORD') AND (destination1 != 'JFK') AND 	((year1 == 2007 AND month1 >= 6) OR (year1 == 2008 AND month1 <= 5));
F2 = FILTER F2 BY (cancelled2 !=1) AND (diverted2 !=1) AND (origin2 != 'ORD') AND (destination2 == 'JFK') AND ((year2 == 2007 AND month2 >= 6) OR (year2 == 2008 AND month2 <= 5));


-- Join F1 and F2 Flights based on flightdate and origin of F2 should be same as origin of F1
Same_Date_OriDes = JOIN F1 BY (destination1,flightDate1) , F2 BY (origin2,flightDate2);
Same_Date_OriDes_Filtered = FILTER Same_Date_OriDes BY (departureTime2 > arrivalTime1);


-- Calculate Delay of all valid flights
delay = FOREACH Same_Date_OriDes_Filtered GENERATE (arrivalDelayMins1 + arrivalDelayMins2) as total_delay;

-- Find AVG of total delay
final = group delay all;

-- Find AVG of total delay
avg = foreach final generate AVG(delay.total_delay);

-- Store the Avg Value 
STORE avg INTO '$OUTPUT';