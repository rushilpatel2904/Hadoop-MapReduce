package pagerank;

import common.FlightDetails;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InitAirportRank {
	/**
	 * 
	 * @author nagarwal
	 *
	 */
	public static class AirportMapper extends
			Mapper<Object, Text, Text, FlightPair> {

		private Map<Text, Map<Text, Integer>> originCount;
		private Set<Text> keySet;

		public void setup(Context context) throws IOException,
				InterruptedException {
			originCount = new HashMap<Text, Map<Text, Integer>>();
			keySet = new HashSet<Text>();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// System.out.println("In mapper");
			Map<Text, Integer> mapValue = new HashMap<Text, Integer>();
			FlightDetails flight = new FlightDetails(value.toString());
			Text destination = flight.getDest();
			Text origin = flight.getOrigin();
			// Check if destination airport exist in map
			if (originCount.containsKey(destination)) {
				mapValue = originCount.get(destination);
				// check if origin exists
				if (mapValue.containsKey(origin))
					// increment count by 1
					mapValue.put(origin, mapValue.get(origin) + 1);
				else
					// initialize count to 1
					mapValue.put(origin, 1);
			} else
				mapValue.put(origin, 1);
			originCount.put(destination, mapValue);
			keySet.add(destination);
			keySet.add(origin);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			// System.out.println("In Cleanup");
			Map<Text, Integer> mapValue = new HashMap<Text, Integer>();
			//emit (destination airport, [origin airport,number of flights]
			for (Text destination : originCount.keySet()) {
				mapValue = originCount.get(destination);
				for (Text origin : mapValue.keySet()) {
					context.write(destination,
							new FlightPair(origin, mapValue.get(origin)));
				}
			}
			for (Text dest : keySet) {
				//emit(*, Airport) -> to count the total number of nodes/airport
				context.write(new Text("*"), new FlightPair(dest, 0));
				if (!originCount.containsKey(dest))
					//emit(source airport,null) to handle dangling nodes
					context.write(new Text(dest.toString()), new FlightPair());
			}
		}
	}

	public static class AirportReducer extends
			Reducer<Text, FlightPair, Text, Text> {

		double pageRank = 0.0;

		public void reduce(Text key, Iterable<FlightPair> values,
				Context context) throws IOException, InterruptedException {
			if (key.toString().equals("*")) {
				//count total no of airports 
				Set<Text> destinations = new HashSet<Text>();
				for (FlightPair flightPair : values) {
					destinations.add(flightPair.getAirport());
				}
				// Initialize pagerank
				pageRank = (double) 1 / (double) destinations.size();
				// System.out.println("PageRank=" + pageRank);
			} else {
				Map<Text, Integer> mapValue = new HashMap<Text, Integer>();
				int total = 0;
				StringBuffer output = new StringBuffer();
				output.append("PageRank," + pageRank);
				for (FlightPair flightPair : values) {
					Text airport = new Text(flightPair.getAirport());
					// System.out.println(airport);
					if (mapValue.containsKey(airport))
						total = flightPair.getCount() + mapValue.get(airport);
					else
						total = flightPair.getCount();
					mapValue.put(airport, total);
				}
				//If not a dangling node, append neighbors along with their count
				if (total > 0) {
					for (Text origin : mapValue.keySet()) {
						output.append(";" + origin + "," + mapValue.get(origin));
					}
				}
				context.write(key, new Text(output.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: arrivalperformance <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "test");
		job.setJarByClass(InitAirportRank.class);
		job.setMapperClass(AirportMapper.class);
		// job.setCombinerClass(AirportReducer.class);
		job.setReducerClass(AirportReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlightPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean b = job.waitForCompletion(true);
		if(!b)
			System.exit(2);
		String[] args2 = {args[1],args[1]};
		PageRank.main(args2);
	}
}
