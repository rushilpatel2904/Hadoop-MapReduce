import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ArrivalPerformanceAirlines {

	// HBase table that stores the average arrival delay
	// for each airline.
	private static String tablename = "average_delay_airlines";
	
	// Table name for HBase.
	private static String sourceTable = "average_delay_airlines";

	// Mapper that outputs key = airlineId and value as it's
	// arrival delay in minutes.
	public static class ArrivalPerfMapper extends
			Mapper<Object, Text, IntWritable, FloatWritable> {
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			FlightDetails flight = new FlightDetails(value.toString());
			IntWritable airlineID = new IntWritable((int)flight.getAirlineID());
			FloatWritable arrDelayMins = new FloatWritable(
					flight.getArrDelayMinutes());
			context.write(airlineID, arrDelayMins);
		}
	}

	// Reducer that computes average arrival delay for each
	// airline and stores them in HBase that sorts the row keys (i.e. average arrival delay)
	public static class ArrivalPerfReducer extends
			Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

		public void reduce(IntWritable key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {

			float sum = 0.0f;
			float total = 0.0f;
			for (FloatWritable value : values) {
				float arrDelayMins = Float.parseFloat(value.toString());
				total += 1;
				if (arrDelayMins <= 0.0f) {
					continue;
				}
				sum += arrDelayMins;
			}

			FloatWritable averageDelayMins = new FloatWritable(sum / total);
			//context.write(key, averageDelayMins);
			try {
				HBaseConnection.addRecord(tablename, sum/total , "airlineID",
						"", key.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	// Reads from HBase and writes to output file.
	public static class ArrivalPerfSortMapper extends
			TableMapper<DoubleWritable, Text> {

		// Column family for hbase named "airlineID".
		public static final byte[] CF = "airlineID".getBytes();
		
		// Empty qualifier.
		public static final byte[] ATTR = "".getBytes();

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {
			
			// convert average delay from Bytes to DoubleWritable
			DoubleWritable averageArrivalDelay = new DoubleWritable(Bytes.toDouble(row.get()));
			byte[] result = value.getValue(CF, ATTR);
			Text airlineID = new Text(Bytes.toString(result));
			System.out.println("Average delay is " + averageArrivalDelay);
			System.out.println("Airline id is " + airlineID);
			context.write(averageArrivalDelay, airlineID);
		}
	}

	// Reducer is turned off.
	public static class ArrivalPerfSortReducer
			extends
			Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		String[] familys = { "airlineID" };
		HBaseConnection.creatTable(tablename, familys);
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: arrivalperformanceairlines <in1> <out1> <out2>");
			System.exit(2);
		}
		Job job = new Job(conf, "arrivalperformanceairlines");
		job.setJarByClass(ArrivalPerformanceAirlines.class);
		job.setMapperClass(ArrivalPerfMapper.class);
		job.setReducerClass(ArrivalPerfReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean job1completed = job.waitForCompletion(true);
		if (!job1completed) {
			System.exit(2);
		}
		
		Configuration config = HBaseConfiguration.create();
		Job job2 = new Job(config, "ArrivalPerformanceReadHBase");
		job2.setJarByClass(ArrivalPerformanceAirlines.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(sourceTable, // input table
				scan, // Scan instance to control CF and attribute selection
				ArrivalPerfSortMapper.class, // mapper class
				DoubleWritable.class, // mapper output key
				Text.class, // mapper output value
				job2);
		job2.setReducerClass(ArrivalPerfSortReducer.class); // reducer class
		job2.setNumReduceTasks(0); // disable reducer, since map-only job
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
