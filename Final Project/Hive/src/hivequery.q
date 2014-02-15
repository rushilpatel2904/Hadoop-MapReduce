-- Add CSV SerDe to the classpath.
add jar s3n://finalproj-puru/lib/csv-serde-1.1.2-0.11.0-all.jar;

-- Create flight table.
CREATE EXTERNAL TABLE IF NOT EXISTS flight(
 Year FLOAT,
 Quarter FLOAT,
 Month FLOAT,
 DayofMonth FLOAT,
 DayOfWeek FLOAT,
 FlightDate STRING,
 UniqueCarrier STRING,
 AirlineID FLOAT,
 Carrier STRING,
 TailNum STRING,
 FlightNum STRING,
 Origin STRING,
 OriginCityName STRING,
 OriginState STRING,
 OriginStateFips STRING,
 OriginStateName STRING,
 OriginWac FLOAT,
 Dest STRING ,
 DestCityName STRING,
 DestState STRING,
 DestStateFips STRING ,
 DestStateName STRING,
 DestWac FLOAT,
 CRSDepTime STRING ,
 DepTime FLOAT ,
 DepDelay FLOAT ,
 DepDelayMinutes FLOAT,
 DepDel15 FLOAT,
 DepartureDelayGroups FLOAT,
 DepTimeBlk STRING ,
 TaxiOut FLOAT,
 WheelsOff STRING,
 WheelsOn STRING,
 TaxiIn FLOAT,
 CRSArrTime STRING,
 ArrTime FLOAT,
 ArrDelay FLOAT,
 ArrDelayMinutes FLOAT,
 ArrDel15 FLOAT,
 ArrivalDelayGroups FLOAT,
 ArrTimeBlk STRING,
 Cancelled FLOAT,
 CancellationCode STRING,
 Diverted FLOAT,
 CRSElapsedTime FLOAT,
 ActualElapsedTime FLOAT,
 AirTime FLOAT,
 Flights FLOAT,
 Distance FLOAT,
 DistanceGroup FLOAT,
 CarrierDelay FLOAT,
 WeatherDelay FLOAT,
 NASDelay FLOAT,
 SecurityDelay FLOAT,
 LateAircraftDelay FLOAT)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE
LOCATION 's3n://finalproj-puru/table/' COMMENT 'Store table in specified location';

-- Load data.csv into flight table
LOAD DATA INPATH 's3n://finalproj-puru/hiveinput/data.csv' INTO TABLE flight;

INSERT OVERWRITE DIRECTORY 's3n://finalproj-puru/hiveoutput2' COMMENT 'Insert outout in a folder on the specified path'
Select a.origin, a.dest, b.dest, c.dest, a.flightdate, a.DepTime, a.ArrTime, b.DepTime, b.ArrTime, c.DepTime, c.ArrTime, (a.actualelapsedtime + b.actualelapsedtime + c.actualelapsedtime + (b.DepTime-a.ArrTime) + (c.DepTime-b.ArrTime)) AS TotalTime from flight a JOIN flight b on (a.dest = b.origin) JOIN flight c on (b.dest = c.origin) where a.flightdate ='2008-01-01' AND b.flightdate = '2008-01-01' AND c.flightdate = '2008-01-01' AND a.origin = 'BOS' AND c.dest = 'BOS'
AND a.DepTime < a.ArrTime
AND b.DepTime < b.ArrTime
AND c.DepTime < c.ArrTime
AND b.DepTime > a.ArrTime
AND c.DepTime > b.ArrTime AND b.DepTime - a.ArrTime > 100 AND c.DepTime - b.ArrTime > 100 
AND a.cancelled != 1 AND b.cancelled != 1 AND b.cancelled != 1
Order by TotalTime LIMIT 20 COMMENT 'Select top 20 records.';
