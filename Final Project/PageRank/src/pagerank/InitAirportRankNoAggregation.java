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

public class InitAirportRankNoAggregation {
	/**
	 * 
	 * @author nagarwal
	 * 
	 */
	public static class AirportMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// System.out.println("In mapper");
			FlightDetails flight = new FlightDetails(value.toString());
			Text destination = flight.getDest();
			Text origin = flight.getOrigin();
			context.write(destination, origin);
			context.write(origin, new Text());
		}
	}

	public static class AirportReducer extends Reducer<Text, Text, Text, Text> {

		double pageRank = 1 / 316.00;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// int total = 0;
			StringBuffer output = new StringBuffer();
			output.append("PageRank," + pageRank);
			for (Text value : values) {
				if (value.getLength() > 0) {
					// total++;
					output.append(";" + value);
				}
			}
			// If not a dangling node, append neighbors along with their
			// count
			context.write(key, new Text(output.toString()));
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
		job.setJarByClass(InitAirportRankNoAggregation.class);
		job.setMapperClass(AirportMapper.class);
		// job.setCombinerClass(AirportReducer.class);
		job.setReducerClass(AirportReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
