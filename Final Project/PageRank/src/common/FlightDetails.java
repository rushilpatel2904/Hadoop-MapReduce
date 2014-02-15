package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import au.com.bytecode.opencsv.CSVParser;

public class FlightDetails implements Writable {

	float Year;
	float Quarter;
	float Month;
	float DayofMonth;
	float DayOfWeek;
	Text FlightDate;
	Text UniqueCarrier;
	float AirlineID;
	Text Carrier;
	Text TailNum;
	Text FlightNum;
	Text Origin;
	Text OriginCityName;
	Text OriginState;
	Text OriginStateFips;
	Text OriginStateName;
	float OriginWac;
	Text Dest;
	Text DestCityName;
	Text DestState;
	Text DestStateFips;
	Text DestStateName;
	float DestWac;
	Text CRSDepTime;
	Text DepTime;
	float DepDelay;
	float DepDelayMinutes;
	float DepDel15;
	float DepartureDelayGroups;
	Text DepTimeBlk;
	float TaxiOut;
	Text WheelsOff;
	Text WheelsOn;
	float TaxiIn;
	Text CRSArrTime;
	Text ArrTime;
	float ArrDelay;
	float ArrDelayMinutes;
	float ArrDel15;
	float ArrivalDelayGroups;
	Text ArrTimeBlk;
	float Cancelled;
	Text CancellationCode;
	float Diverted;
	float CRSElapsedTime;
	float ActualElapsedTime;
	float AirTime;
	float Flights;
	float Distance;
	float DistanceGroup;
	float CarrierDelay;
	float WeatherDelay;
	float NASDelay;
	float SecurityDelay;
	float LateAircraftDelay;

	public FlightDetails() {
		FlightDate = new Text();
		FlightDate = new Text();
		UniqueCarrier = new Text();
		Carrier = new Text();
		TailNum = new Text();
		FlightNum = new Text();
		Origin = new Text();
		OriginCityName = new Text();
		OriginState = new Text();
		OriginStateFips = new Text();
		OriginStateName = new Text();
		Dest = new Text();
		DestCityName = new Text();
		DestState = new Text();
		DestStateFips = new Text();
		DestStateName = new Text();
		CRSDepTime = new Text();
		DepTime = new Text();
		DepTimeBlk = new Text();
		WheelsOff = new Text();
		WheelsOn = new Text();
		CRSArrTime = new Text();
		ArrTime = new Text();
		ArrTimeBlk = new Text();
		CancellationCode = new Text();

	}

	public float getYear() {
		return Year;
	}

	public void setYear(float year) {
		Year = year;
	}

	public float getQuarter() {
		return Quarter;
	}

	public void setQuarter(float quarter) {
		Quarter = quarter;
	}

	public float getMonth() {
		return Month;
	}

	public void setMonth(float month) {
		Month = month;
	}

	public float getDayofMonth() {
		return DayofMonth;
	}

	public void setDayofMonth(float dayofMonth) {
		DayofMonth = dayofMonth;
	}

	public float getDayOfWeek() {
		return DayOfWeek;
	}

	public void setDayOfWeek(float dayOfWeek) {
		DayOfWeek = dayOfWeek;
	}

	public Text getFlightDate() {
		return FlightDate;
	}

	public void setFlightDate(Text flightDate) {
		FlightDate = flightDate;
	}

	public Text getUniqueCarrier() {
		return UniqueCarrier;
	}

	public void setUniqueCarrier(Text uniqueCarrier) {
		UniqueCarrier = uniqueCarrier;
	}

	public float getAirlineID() {
		return AirlineID;
	}

	public void setAirlineID(float airlineID) {
		AirlineID = airlineID;
	}

	public Text getCarrier() {
		return Carrier;
	}

	public void setCarrier(Text carrier) {
		Carrier = carrier;
	}

	public Text getTailNum() {
		return TailNum;
	}

	public void setTailNum(Text tailNum) {
		TailNum = tailNum;
	}

	public Text getFlightNum() {
		return FlightNum;
	}

	public void setFlightNum(Text flightNum) {
		FlightNum = flightNum;
	}

	public Text getOrigin() {
		return Origin;
	}

	public void setOrigin(Text origin) {
		Origin = origin;
	}

	public Text getOriginCityName() {
		return OriginCityName;
	}

	public void setOriginCityName(Text originCityName) {
		OriginCityName = originCityName;
	}

	public Text getOriginState() {
		return OriginState;
	}

	public void setOriginState(Text originState) {
		OriginState = originState;
	}

	public Text getOriginStateFips() {
		return OriginStateFips;
	}

	public void setOriginStateFips(Text originStateFips) {
		OriginStateFips = originStateFips;
	}

	public Text getOriginStateName() {
		return OriginStateName;
	}

	public void setOriginStateName(Text originStateName) {
		OriginStateName = originStateName;
	}

	public float getOriginWac() {
		return OriginWac;
	}

	public void setOriginWac(float originWac) {
		OriginWac = originWac;
	}

	public Text getDest() {
		return Dest;
	}

	public void setDest(Text dest) {
		Dest = dest;
	}

	public Text getDestCityName() {
		return DestCityName;
	}

	public void setDestCityName(Text destCityName) {
		DestCityName = destCityName;
	}

	public Text getDestState() {
		return DestState;
	}

	public void setDestState(Text destState) {
		DestState = destState;
	}

	public Text getDestStateFips() {
		return DestStateFips;
	}

	public void setDestStateFips(Text destStateFips) {
		DestStateFips = destStateFips;
	}

	public Text getDestStateName() {
		return DestStateName;
	}

	public void setDestStateName(Text destStateName) {
		DestStateName = destStateName;
	}

	public float getDestWac() {
		return DestWac;
	}

	public void setDestWac(float destWac) {
		DestWac = destWac;
	}

	public Text getCRSDepTime() {
		return CRSDepTime;
	}

	public void setCRSDepTime(Text cRSDepTime) {
		CRSDepTime = cRSDepTime;
	}

	public Text getDepTime() {
		return DepTime;
	}

	public void setDepTime(Text depTime) {
		DepTime = depTime;
	}

	public float getDepDelay() {
		return DepDelay;
	}

	public void setDepDelay(float depDelay) {
		DepDelay = depDelay;
	}

	public float getDepDelayMinutes() {
		return DepDelayMinutes;
	}

	public void setDepDelayMinutes(float depDelayMinutes) {
		DepDelayMinutes = depDelayMinutes;
	}

	public float getDepDel15() {
		return DepDel15;
	}

	public void setDepDel15(float depDel15) {
		DepDel15 = depDel15;
	}

	public float getDepartureDelayGroups() {
		return DepartureDelayGroups;
	}

	public void setDepartureDelayGroups(float departureDelayGroups) {
		DepartureDelayGroups = departureDelayGroups;
	}

	public Text getDepTimeBlk() {
		return DepTimeBlk;
	}

	public void setDepTimeBlk(Text depTimeBlk) {
		DepTimeBlk = depTimeBlk;
	}

	public float getTaxiOut() {
		return TaxiOut;
	}

	public void setTaxiOut(float taxiOut) {
		TaxiOut = taxiOut;
	}

	public Text getWheelsOff() {
		return WheelsOff;
	}

	public void setWheelsOff(Text wheelsOff) {
		WheelsOff = wheelsOff;
	}

	public Text getWheelsOn() {
		return WheelsOn;
	}

	public void setWheelsOn(Text wheelsOn) {
		WheelsOn = wheelsOn;
	}

	public float getTaxiIn() {
		return TaxiIn;
	}

	public void setTaxiIn(float taxiIn) {
		TaxiIn = taxiIn;
	}

	public Text getCRSArrTime() {
		return CRSArrTime;
	}

	public void setCRSArrTime(Text cRSArrTime) {
		CRSArrTime = cRSArrTime;
	}

	public Text getArrTime() {
		return ArrTime;
	}

	public void setArrTime(Text arrTime) {
		ArrTime = arrTime;
	}

	public float getArrDelay() {
		return ArrDelay;
	}

	public void setArrDelay(float arrDelay) {
		ArrDelay = arrDelay;
	}

	public float getArrDelayMinutes() {
		return ArrDelayMinutes;
	}

	public void setArrDelayMinutes(float arrDelayMinutes) {
		ArrDelayMinutes = arrDelayMinutes;
	}

	public float getArrDel15() {
		return ArrDel15;
	}

	public void setArrDel15(float arrDel15) {
		ArrDel15 = arrDel15;
	}

	public float getArrivalDelayGroups() {
		return ArrivalDelayGroups;
	}

	public void setArrivalDelayGroups(float arrivalDelayGroups) {
		ArrivalDelayGroups = arrivalDelayGroups;
	}

	public Text getArrTimeBlk() {
		return ArrTimeBlk;
	}

	public void setArrTimeBlk(Text arrTimeBlk) {
		ArrTimeBlk = arrTimeBlk;
	}

	public float getCancelled() {
		return Cancelled;
	}

	public void setCancelled(float cancelled) {
		Cancelled = cancelled;
	}

	public Text getCancellationCode() {
		return CancellationCode;
	}

	public void setCancellationCode(Text cancellationCode) {
		CancellationCode = cancellationCode;
	}

	public float getDiverted() {
		return Diverted;
	}

	public void setDiverted(float diverted) {
		Diverted = diverted;
	}

	public float getCRSElapsedTime() {
		return CRSElapsedTime;
	}

	public void setCRSElapsedTime(float cRSElapsedTime) {
		CRSElapsedTime = cRSElapsedTime;
	}

	public float getActualElapsedTime() {
		return ActualElapsedTime;
	}

	public void setActualElapsedTime(float actualElapsedTime) {
		ActualElapsedTime = actualElapsedTime;
	}

	public float getAirTime() {
		return AirTime;
	}

	public void setAirTime(float airTime) {
		AirTime = airTime;
	}

	public float getFlights() {
		return Flights;
	}

	public void setFlights(float flights) {
		Flights = flights;
	}

	public float getDistance() {
		return Distance;
	}

	public void setDistance(float distance) {
		Distance = distance;
	}

	public float getDistanceGroup() {
		return DistanceGroup;
	}

	public void setDistanceGroup(float distanceGroup) {
		DistanceGroup = distanceGroup;
	}

	public float getCarrierDelay() {
		return CarrierDelay;
	}

	public void setCarrierDelay(float carrierDelay) {
		CarrierDelay = carrierDelay;
	}

	public float getWeatherDelay() {
		return WeatherDelay;
	}

	public void setWeatherDelay(float weatherDelay) {
		WeatherDelay = weatherDelay;
	}

	public float getNASDelay() {
		return NASDelay;
	}

	public void setNASDelay(float nASDelay) {
		NASDelay = nASDelay;
	}

	public float getSecurityDelay() {
		return SecurityDelay;
	}

	public void setSecurityDelay(float securityDelay) {
		SecurityDelay = securityDelay;
	}

	public float getLateAircraftDelay() {
		return LateAircraftDelay;
	}

	public void setLateAircraftDelay(float lateAircraftDelay) {
		LateAircraftDelay = lateAircraftDelay;
	}

	// Unimplemented methods of Interface Writable

	@Override
	public void readFields(DataInput in) throws IOException {
		Year = in.readFloat();
		Quarter = in.readFloat();
		Month = in.readFloat();
		DayofMonth = in.readFloat();
		DayOfWeek = in.readFloat();

		FlightDate.readFields(in);
		UniqueCarrier.readFields(in);
		AirlineID = in.readFloat();
		Carrier.readFields(in);
		TailNum.readFields(in);
		FlightNum.readFields(in);
		Origin.readFields(in);
		OriginCityName.readFields(in);
		OriginState.readFields(in);
		OriginStateFips.readFields(in);
		OriginStateName.readFields(in);
		OriginWac = in.readFloat();
		Dest.readFields(in);
		DestCityName.readFields(in);
		DestState.readFields(in);
		DestStateFips.readFields(in);
		DestStateName.readFields(in);
		DestWac = in.readFloat();
		CRSDepTime.readFields(in);
		DepTime.readFields(in);
		DepDelay = in.readFloat();
		DepDelayMinutes = in.readFloat();
		DepDel15 = in.readFloat();
		DepartureDelayGroups = in.readFloat();
		DepTimeBlk.readFields(in);
		TaxiOut = in.readFloat();
		WheelsOff.readFields(in);
		WheelsOn.readFields(in);
		TaxiIn = in.readFloat();
		CRSArrTime.readFields(in);
		ArrTime.readFields(in);
		ArrDelay = in.readFloat();
		ArrDelayMinutes = in.readFloat();
		ArrDel15 = in.readFloat();
		ArrivalDelayGroups = in.readFloat();
		ArrTimeBlk.readFields(in);
		Cancelled = in.readFloat();
		CancellationCode.readFields(in);
		Diverted = in.readFloat();
		CRSElapsedTime = in.readFloat();
		ActualElapsedTime = in.readFloat();
		AirTime = in.readFloat();
		Flights = in.readFloat();
		Distance = in.readFloat();
		DistanceGroup = in.readFloat();
		CarrierDelay = in.readFloat();
		WeatherDelay = in.readFloat();
		NASDelay = in.readFloat();
		SecurityDelay = in.readFloat();
		LateAircraftDelay = in.readFloat();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(Year);
		out.writeFloat(Quarter);
		out.writeFloat(Month);
		out.writeFloat(DayofMonth);
		out.writeFloat(DayOfWeek);
		FlightDate.write(out);
		UniqueCarrier.write(out);
		out.writeFloat(AirlineID);
		Carrier.write(out);
		TailNum.write(out);
		FlightNum.write(out);
		Origin.write(out);
		OriginCityName.write(out);
		OriginState.write(out);
		OriginStateFips.write(out);
		OriginStateName.write(out);
		out.writeFloat(OriginWac);
		Dest.write(out);
		DestCityName.write(out);
		DestState.write(out);
		DestStateFips.write(out);
		DestStateName.write(out);
		out.writeFloat(DestWac);
		CRSDepTime.write(out);
		DepTime.write(out);
		out.writeFloat(DepDelay);
		out.writeFloat(DepDelayMinutes);
		out.writeFloat(DepDel15);
		out.writeFloat(DepartureDelayGroups);
		DepTimeBlk.write(out);
		out.writeFloat(TaxiOut);
		WheelsOff.write(out);
		WheelsOn.write(out);
		out.writeFloat(TaxiIn);
		CRSArrTime.write(out);
		ArrTime.write(out);
		out.writeFloat(ArrDelay);
		out.writeFloat(ArrDelayMinutes);
		out.writeFloat(ArrDel15);
		out.writeFloat(ArrivalDelayGroups);
		ArrTimeBlk.write(out);
		out.writeFloat(Cancelled);
		CancellationCode.write(out);
		out.writeFloat(Diverted);
		out.writeFloat(CRSElapsedTime);
		out.writeFloat(ActualElapsedTime);
		out.writeFloat(AirTime);
		out.writeFloat(Flights);
		out.writeFloat(Distance);
		out.writeFloat(DistanceGroup);
		out.writeFloat(CarrierDelay);
		out.writeFloat(WeatherDelay);
		out.writeFloat(NASDelay);
		out.writeFloat(SecurityDelay);
		out.writeFloat(LateAircraftDelay);

	}

	public FlightDetails(String flightData) {
		CSVParser csvParser = new CSVParser();
		String[] flightArray = null;
		try {
			flightArray = csvParser.parseLine(flightData);
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < flightArray.length; i++) {
			if (flightArray[i].equals(null) || flightArray[i].equals("")) {
				flightArray[i] = "-999.0";
			}
		}

		Year = Float.parseFloat(flightArray[0]);
		Quarter = Float.parseFloat(flightArray[1]);
		Month = Float.parseFloat(flightArray[2]);
		DayofMonth = Float.parseFloat(flightArray[3]);
		DayOfWeek = Float.parseFloat(flightArray[4]);
		FlightDate = new Text(flightArray[5]);
		UniqueCarrier = new Text(flightArray[6]);
		AirlineID = Float.parseFloat(flightArray[7]);
		Carrier = new Text(flightArray[8]);
		TailNum = new Text(flightArray[9]);
		FlightNum = new Text(flightArray[10]);
		Origin = new Text(flightArray[11]);
		OriginCityName = new Text(flightArray[12]);
		OriginState = new Text(flightArray[13]);
		OriginStateFips = new Text(flightArray[14]);
		OriginStateName = new Text(flightArray[15]);
		OriginWac = Float.parseFloat(flightArray[16]);
		Dest = new Text(flightArray[17]);
		DestCityName = new Text(flightArray[18]);
		DestState = new Text(flightArray[19]);
		DestStateFips = new Text(flightArray[20]);
		DestStateName = new Text(flightArray[21]);
		DestWac = Float.parseFloat(flightArray[22]);
		CRSDepTime = new Text(flightArray[23]);
		DepTime = new Text(flightArray[24]);
		DepDelay = Float.parseFloat(flightArray[25]);
		DepDelayMinutes = Float.parseFloat(flightArray[26]);
		DepDel15 = Float.parseFloat(flightArray[27]);
		DepartureDelayGroups = Float.parseFloat(flightArray[28]);
		DepTimeBlk = new Text(flightArray[29]);
		TaxiOut = Float.parseFloat(flightArray[30]);
		WheelsOff = new Text(flightArray[31]);
		WheelsOn = new Text(flightArray[32]);
		TaxiIn = Float.parseFloat(flightArray[33]);
		CRSArrTime = new Text(flightArray[34]);
		ArrTime = new Text(flightArray[35]);
		ArrDelay = Float.parseFloat(flightArray[36]);
		ArrDelayMinutes = Float.parseFloat(flightArray[37]);
		ArrDel15 = Float.parseFloat(flightArray[38]);
		ArrivalDelayGroups = Float.parseFloat(flightArray[39]);
		ArrTimeBlk = new Text(flightArray[40]);
		Cancelled = Float.parseFloat(flightArray[41]);
		CancellationCode = new Text(flightArray[42]);
		Diverted = Float.parseFloat(flightArray[43]);
		CRSElapsedTime = Float.parseFloat(flightArray[44]);
		ActualElapsedTime = Float.parseFloat(flightArray[45]);
		AirTime = Float.parseFloat(flightArray[46]);
		Flights = Float.parseFloat(flightArray[47]);
		Distance = Float.parseFloat(flightArray[48]);
		DistanceGroup = Float.parseFloat(flightArray[49]);
		CarrierDelay = Float.parseFloat(flightArray[50]);
		WeatherDelay = Float.parseFloat(flightArray[51]);
		NASDelay = Float.parseFloat(flightArray[52]);
		SecurityDelay = Float.parseFloat(flightArray[53]);
		LateAircraftDelay = Float.parseFloat(flightArray[54]);

	}

	public String toString() {
		String x = Year + "," + Quarter + "," + Month + "," + DayofMonth + ","
				+ DayOfWeek + "," + FlightDate + "," + UniqueCarrier + ","
				+ AirlineID + "," + Carrier + "," + TailNum + "," + FlightNum
				+ "," + Origin + "," + OriginCityName + "," + OriginState + ","
				+ OriginStateFips + "," + OriginStateName + "," + OriginWac
				+ "," + Dest + "," + DestCityName + "," + DestState + ","
				+ DestStateFips + "," + DestStateName + "," + DestWac + ","
				+ CRSDepTime + "," + DepTime + "," + DepDelay + ","
				+ DepDelayMinutes + "," + DepDel15 + "," + DepartureDelayGroups
				+ "," + DepTimeBlk + "," + TaxiOut + "," + WheelsOff + ","
				+ WheelsOn + "," + TaxiIn + "," + CRSArrTime + "," + ArrTime
				+ "," + ArrDelay + "," + ArrDelayMinutes + "," + ArrDel15 + ","
				+ ArrivalDelayGroups + "," + ArrTimeBlk + "," + Cancelled + ","
				+ CancellationCode + "," + Diverted + "," + CRSElapsedTime
				+ "," + ActualElapsedTime + "," + AirTime + "," + Flights + ","
				+ Distance + "," + DistanceGroup + "," + CarrierDelay + ","
				+ WeatherDelay + "," + NASDelay + "," + SecurityDelay + ","
				+ LateAircraftDelay;

		return x;
	}

}
