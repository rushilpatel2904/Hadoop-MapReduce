-- To use piggybank.jar and CSVLoader()
REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Set default parallel
SET default_parallel 10

-- Load flight data
Flights1 = load '$INPUT' USING CSVLoader();
Flights2 = load '$INPUT' USING CSVLoader();
Flights3 = load '$INPUT' USING CSVLoader();

--Flights1 = load '/Users/purushottamjha/Desktop/data1.csv' USING CSVLoader();
--Flights2 = load '/Users/purushottamjha/Desktop/data1.csv' USING CSVLoader();
--Flights3 = load '/Users/purushottamjha/Desktop/data1.csv' USING CSVLoader();

-- Flights1_Data contains necessary fields from Flights 1. 
Flights1_Data = FOREACH Flights1 GENERATE (chararray)$5 as flightDate1, (chararray)$11 as orig1, (chararray)$17 as dest1, (int)$24 as depTime1, (int)$35 as arrTime1, (int)$45  as actualElapsedTime1, (int)$41 as cancelled1;

-- Flights1_Data contains necessary fields from Flights 2. 
Flights2_Data = FOREACH Flights2 GENERATE (chararray)$5 as flightDate2, (chararray)$11 as orig2, (chararray)$17 as dest2, (int)$24 as depTime2,(int)$35 as arrTime2, (int)$45  as actualElapsedTime2, (int)$41 as cancelled2;

-- Flights3_Data contains necessary fields from Flights 3. 
Flights3_Data = FOREACH Flights3 GENERATE (chararray)$5 as flightDate3, (chararray)$11 as orig3, (chararray)$17 as dest3, (int)$24 as depTime3,(int)$35 as arrTime3, (int)$45  as actualElapsedTime3, (int)$41 as cancelled3;


Flights1_Data = FILTER Flights1_Data by (orig1 == 'BOS') AND (flightDate1 == '2008-01-01') AND (cancelled1 != 1);
Flights2_Data = FILTER Flights2_Data by (flightDate2 == '2008-01-01') AND (cancelled2 != 1);
Flights3_Data = FILTER Flights3_Data by  (dest3 == 'BOS') AND (flightDate3 == '2008-01-01') AND (cancelled3 != 1);

f1f2 = JOIN Flights1_Data BY (dest1), Flights2_Data BY (orig2);
f1f2 = FILTER f1f2 BY depTime2 > arrTime1;

f1f2f3 = JOIN f1f2 BY (dest2), Flights3_Data BY (orig3);
f1f2f3 = FILTER f1f2f3 BY  depTime3 > arrTime2;

f1f2f3 = FILTER f1f2f3 BY ((depTime2-arrTime1) > 100) AND ((depTime3-arrTime2) > 100);


final = FOREACH f1f2f3 GENERATE orig1,dest1,dest2,dest3,flightDate1,depTime1,arrTime1,depTime2,arrTime2,depTime3,arrTime3,  (actualElapsedTime1 + actualElapsedTime2 + actualElapsedTime3 + (depTime2 - arrTime1) + (depTime3 - arrTime2)) AS totalTripTime;

final = ORDER final BY totalTripTime;

final = limit final 20;

-- STORE final INTO 'out1';
STORE final INTO '$OUTPUT';



