package pagerank;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import org.hsqldb.lib.HashMap;

public class PageRank {

	private static double alpha = 0.1;
	private static double totalNumberOfFlights = 316.0;
	private static Map<Text, Double> oldPageRank = new HashMap<Text, Double>();
	private static Map<Text, Double> newPageRank = new HashMap<Text, Double>();

	public static class PageRankMapper extends
			Mapper<Object, Text, Text, FlightNode> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			FlightNode node = new FlightNode(value.toString());
			Text nodeId = node.getNodeId();
			double pagerank = node.getPageRank();
			oldPageRank.put(nodeId, pagerank);
			context.write(nodeId, node);
			double p = pagerank / (double) node.getIncomingLinksCount();
			for (Writable id : node.getNeighbors().keySet()) {
				context.write(new Text(id.toString()), new FlightNode(p));
			}
		}
	}

	public static class PageRankReducer extends
			Reducer<Text, FlightNode, Text, Text> {
		public void reduce(Text key, Iterable<FlightNode> values,
				Context context) throws IOException, InterruptedException {
			double sumPageRank = 0.0;
			FlightNode flightNode = new FlightNode();
			for (FlightNode node : values) {
				if (node.isNode())
					flightNode = node; // deep copy may be required here
				else
					sumPageRank += node.getPageRank();
			}
			double updatedPageRank = ((alpha / totalNumberOfFlights) + ((1.0 - alpha) * sumPageRank));
			// System.out.println(key.toString() + "=" + updatedPageRank);
			flightNode.setPageRank(updatedPageRank);
			newPageRank.put(new Text(key.toString()), updatedPageRank);
			context.write(key, new Text(flightNode.toString()));
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
		boolean exitFlag = true;
		String inputPath = otherArgs[0];
		int counter = 1;
		String outputPath = otherArgs[1] + counter;
		while (!exitFlag) {
			exitFlag = true;
			Job job = new Job(conf, "PageRank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			// job.setCombinerClass(AirportReducer.class);
			job.setReducerClass(PageRankReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlightNode.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputPath.toString()));
			FileOutputFormat
					.setOutputPath(job, new Path(outputPath.toString()));
			boolean b = job.waitForCompletion(true);
			if (!b) {
				exitFlag = false;
				break;
			}
			for (Text id : oldPageRank.keySet()) {
				double oldpagerank = oldPageRank.get(id);
				double newpagerank = newPageRank.get(id);
				if (oldpagerank != newpagerank) {
					exitFlag = false;
					break;
				}
			}
			if (!exitFlag) {
				inputPath = outputPath;
				counter++;
				outputPath = otherArgs[1] + counter;
			}
		}
		System.exit(exitFlag ? 0 : 1);
	}
}
