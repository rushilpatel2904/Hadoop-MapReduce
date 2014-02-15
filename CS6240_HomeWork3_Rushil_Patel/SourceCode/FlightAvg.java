/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import au.com.bytecode.opencsv.CSVParser;

// Reference class to convert each link of input 
// into object for easy access
class Flight1 {

	public int getYear() {
		return Year;
	}

	public void setYear(int year) {
		Year = year;
	}

	public int getMonth() {
		return Month;
	}

	public void setMonth(int month) {
		Month = month;
	}

	public String getFlightDate() {
		return FlightDate;
	}

	public void setFlightDate(String flightDate) {
		FlightDate = flightDate;
	}

	public String getOrigin() {
		return Origin;
	}

	public void setOrigin(String origin) {
		Origin = origin;
	}

	public String getDesination() {
		return Desination;
	}

	public void setDesination(String desination) {
		Desination = desination;
	}

	public int getDepartTime() {
		return DepartTime;
	}

	public void setDepartTime(int departTime) {
		DepartTime = departTime;
	}

	public int getArrivalTime() {
		return ArrivalTime;
	}

	public void setArrivalTime(int arrivalTime) {
		ArrivalTime = arrivalTime;
	}

	public float getArrivalDelayMins() {
		return ArrivalDelayMins;
	}

	public void setArrivalDelayMins(float arrivalDelayMins) {
		ArrivalDelayMins = arrivalDelayMins;
	}

	public float getCancelled() {
		return Cancelled;
	}

	public void setCancelled(float cancelled) {
		Cancelled = cancelled;
	}

	public int getLeg() {
		return leg;
	}

	public void setLeg(int leg) {
		this.leg = leg;
	}

	public float getDiverted() {
		return Diverted;
	}

	public void setDiverted(float diverted) {
		Diverted = diverted;
	}

	public boolean isComplete() {
		return isComplete;
	}

	// Check if any of the fields are empty
	public void setComplete(boolean isComplete) {
		this.isComplete = isComplete;
	}

	int Year;
	int Month;
	String FlightDate;
	String Origin;
	String Desination;
	int DepartTime;
	int ArrivalTime;
	float ArrivalDelayMins;
	float Cancelled;
	int leg;
	float Diverted;
	boolean isComplete = true;

	// Base Constructor used for Mapper
	public Flight1(int year, int month, String flightDate, String origin,
			String desination, int departTime, int arrivalTime,
			float arrivalDelayMins, float cancelled, int leg, float diverted,
			boolean isComplete) {
		super();
		Year = year;
		Month = month;
		FlightDate = flightDate;
		Origin = origin;
		Desination = desination;
		DepartTime = departTime;
		ArrivalTime = arrivalTime;
		ArrivalDelayMins = arrivalDelayMins;
		Cancelled = cancelled;
		this.leg = leg;
		Diverted = diverted;
		this.isComplete = isComplete;
	}

	// Constructor for Reduce
	public Flight1(String[] x) {
		DepartTime = Integer.parseInt(x[0]);
		ArrivalTime = Integer.parseInt(x[1]);
		ArrivalDelayMins = Float.parseFloat(x[2]);
		leg = Integer.parseInt(x[3]);
	}

	// Converts the required fields from mapper to reducer
	public String toString() {
		return DepartTime + "," + ArrivalTime + ","
				+ Float.toString(ArrivalDelayMins) + ","
				+ Integer.toString(leg);
	}
}

// Start Class
public class FlightAvg {

	public enum MyCounters {
		Sum, Total
	}

	// Mappe for First Lef
	public static class FirstLegMapper extends Mapper<Object, Text, Text, Text> {

		// Takes Input of object as Key and Text as value.
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parser for parsing CSV File
			CSVParser parser = new CSVParser();

			// Date formatter for comparing and parsing Dates
			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");

			// Parse Input Line
			String[] line = parser.parseLine(value.toString());

			// Create String Builder
			StringBuilder b = new StringBuilder();

			// Extract date and append it to the ouput key
			b.append(line[5]);

			// Append , to ouput key
			b.append(",");

			// Create new Object for Flight1 and pass details of Flight from
			// Parsed row
			Flight1 f = new Flight1(line[0].equals("") ? 0
					: Integer.parseInt(line[0]), line[2].equals("") ? 0
					: Integer.parseInt(line[2]), line[5], line[11], line[17],
					line[24].equals("") ? 0 : Integer.parseInt(line[24]),
					line[35].equals("") ? 0 : Integer.parseInt(line[35]),
					line[37].equals("") ? 0 : Float.parseFloat(line[37]),
					line[41].equals("") ? 0 : Float.parseFloat(line[41]), 0,
					line[43].equals("") ? 0 : Float.parseFloat(line[43]), true);

			// Check for valid Flights
			if (f.getDiverted() != 1.0 && f.getCancelled() != 1.0) {
				try {
					if ((dt.parse("2007-06-01").compareTo(
							dt.parse(f.getFlightDate())) <= 0)
							&& (dt.parse("2008-05-31").compareTo(
									dt.parse(f.getFlightDate())) >= 0)) {
						if (f.getOrigin().equals("ORD")
								&& !f.getDesination().equals("JFK")) {

							// If flight is valid append Destination of current
							// flight to the key
							b.append(f.getDesination());
							f.setLeg(1);

							// Emit key [date,destination] and value [details of
							// flight]
							context.write(new Text(b.toString()),
									new Text(f.toString()));
						}
					}
				} catch (Exception e) {

				}
			}
		}

	}

	// Mapper for second leg
	public static class SecondLegMapper extends
			Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parser for parsing flight detials
			CSVParser parser = new CSVParser();

			// Date formatter for parsing and comparing dates
			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");

			// Parse input line
			String[] line = parser.parseLine(value.toString());

			// String builder to create new key
			StringBuilder b = new StringBuilder();

			// Append Date
			b.append(line[5]);

			// Append , to key
			b.append(",");

			// Create new Flight object using parsed data
			Flight1 f = new Flight1(line[0].equals("") ? 0
					: Integer.parseInt(line[0]), line[2].equals("") ? 0
					: Integer.parseInt(line[2]), line[5], line[11], line[17],
					line[24].equals("") ? 0 : Integer.parseInt(line[24]),
					line[35].equals("") ? 0 : Integer.parseInt(line[35]),
					line[37].equals("") ? 0 : Float.parseFloat(line[37]),
					line[41].equals("") ? 0 : Float.parseFloat(line[41]), 0,
					line[43].equals("") ? 0 : Float.parseFloat(line[43]), true);

			// Check for Flight validity for second leg flights
			if (f.getDiverted() != 1.0 && f.getCancelled() != 1.0) {
				try {
					if ((dt.parse("2007-06-01").compareTo(
							dt.parse(f.getFlightDate())) <= 0)
							&& (dt.parse("2008-05-31").compareTo(
									dt.parse(f.getFlightDate())) >= 0)) {
						if (!f.getOrigin().equals("ORD")
								&& f.getDesination().equals("JFK")) {

							// If flight is valid second leg flight
							b.append(f.getOrigin());

							// Append Origin
							f.setLeg(2);

							// Emite key [date,origin] and value [flight
							// details]
							context.write(new Text(b.toString()),
									new Text(f.toString()));
						}
					}
				} catch (Exception e) {

				}

			}

		}

	}

	// Reducer takes key [date,destination/origin] and value as flight details
	public static class IntSumReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Set sum = 0
			int sum = 0;

			// List to hold all Left Leg Flights
			ArrayList<Flight1> l1 = new ArrayList<Flight1>();

			// List to hold all right Leg Flights
			ArrayList<Flight1> l2 = new ArrayList<Flight1>();

			// Iterate through all the values and seperate left leg and right
			// leg
			for (Text x : values) {
				Flight1 f = new Flight1((x.toString()).split(","));
				if (f.leg == 1) {
					l1.add(f);
				} else {
					if (f.leg == 2) {
						l2.add(f);
					}
				}

			}

			// Check condition for depart time of second leg flight > arrival
			// time of first leg flight
			for (Flight1 FirstLeg : l1) {
				for (Flight1 SecondLeg : l2) {
					if (SecondLeg.getDepartTime() > FirstLeg.getArrivalTime()) {

						// Add Delyas
						float delay = FirstLeg.getArrivalDelayMins()
								+ SecondLeg.getArrivalDelayMins();
						// Increment total delay counter
						context.getCounter(MyCounters.Total).increment(
								(long) delay);

						// Increment total flight count counter
						context.getCounter(MyCounters.Sum).increment(1);
					}
				}
			}
			context.write(new Text(key), new IntWritable(sum));
		}
	}

	// Main Method
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <in> <out>");
			System.exit(2);
		}

		// Set job
		Job job = new Job(conf, "Flight count");

		// Set number of reduce tasks 10
		job.setNumReduceTasks(10);

		// Set jar by class
		job.setJarByClass(FlightAvg.class);

		// Set reducer class
		job.setReducerClass(IntSumReducer.class);

		// Set Map Output Key
		job.setMapOutputKeyClass(Text.class);

		// Set Map output Value
		job.setMapOutputValueClass(Text.class);

		// Set Output Key
		job.setOutputKeyClass(Text.class);

		// Set Outpur Value
		job.setOutputValueClass(IntWritable.class);

		// Set Mapper Class
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, FirstLegMapper.class);

		// Set Reducer Class
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, SecondLegMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		// Wait for completing job
		boolean jobCompleteFlag = job.waitForCompletion(true);

		// Get counter values
		Counters counters = job.getCounters();

		// Get sum
		double sum = counters.findCounter(MyCounters.Sum).getValue();
		System.out.println("Counter Sum:" + sum);

		// Get total delay
		double totalDelay = counters.findCounter(MyCounters.Total).getValue();

		// Prink Solution
		System.out.println("Counter TotalDelay:" + totalDelay);
		System.out.println("Avg Delay:" + totalDelay / sum);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
