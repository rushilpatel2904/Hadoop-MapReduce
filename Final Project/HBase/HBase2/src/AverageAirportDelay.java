import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageAirportDelay {

	// HBase table that stores the average arrival delay
	// at each airport.
	private static String tablename = "average_arrival_delay_airport1";

	// Mapper that outputs key = airport and value as it's
	// arrival delay in minutes.
	public static class AverageAirportDelayMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			FlightDetails flight = new FlightDetails(value.toString());
			Text destination = new Text(flight.getDestCityName());
			FloatWritable arrDelayMins = new FloatWritable(
					(float) flight.getArrDelayMinutes());
			context.write(destination, arrDelayMins);
		}
	}

	// Reducer that computes average arrival delay for each
	// airport and stores them in HBase.
	public static class AverageAirportDelayReducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {

			float sum = 0.0f;
			float total = 0.0f;
			for (FloatWritable value : values) {
				float arrDelayMins = Float.parseFloat(value.toString());
				total += 1;
				// check for values which were null and changed to -999
				if (arrDelayMins <= 0.0f) {
					continue;
				}
				sum += arrDelayMins;
			}

			FloatWritable averageDelayMins = new FloatWritable(sum / total);
			context.write(key, averageDelayMins);
			try {
				HBaseConnection.addRecord(tablename, key.toString(), "delay",
						"", averageDelayMins.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {

		String[] familys = { "delay" };
		HBaseConnection.creatTable(tablename, familys);
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: arrivalperformanceairport <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "arrivalperformanceairport");
		job.setJarByClass(AverageAirportDelay.class);
		job.setMapperClass(AverageAirportDelayMapper.class);
		job.setReducerClass(AverageAirportDelayReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
