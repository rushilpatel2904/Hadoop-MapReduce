package defaul;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HubsAndSpokes {
	public enum Mycounters {
		spokeNorm, hubNorm
	}

	public static class HubSpokeLoadMapper extends
			Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			FlightDetails flight = new FlightDetails(value.toString());
			Text origin = flight.getOrigin();
			Text destination = flight.getDest();

			context.write(origin, new Text("Out," + destination.toString()));
			context.write(destination, new Text("In," + origin.toString()));
		}
	}

	public static class HubSpokeLoadReducer extends
			Reducer<Text, Text, NodeWritable, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			NodeWritable n = new NodeWritable();
			n.setNid(key);

			for (Text value : values) {
				String[] valueArray = value.toString().split(",");
				String flag = valueArray[0];
				if (flag.equals("In")) {
					NodeWritable x = new NodeWritable();
					x.setNid(new Text(valueArray[1]));
					n.addInNode(x);
				} else {
					NodeWritable x = new NodeWritable();
					x.setNid(new Text(valueArray[1]));
					n.addOutNode(x);
				}
			}
			context.write(n, new Text());
		}
	}

	public static void main(String[] args) throws Exception {
		
		/// Create JOB 1 to convert all the flight data in to NODE : Graph Structure.
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: hubsandspokesload <in> <out> <finalout>");
			System.exit(2);
		}
		Job job = new Job(conf, "hubsandspokesload");
		job.setJarByClass(HubsAndSpokes.class);
		job.setMapperClass(HubSpokeLoadMapper.class);
		job.setReducerClass(HubSpokeLoadReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NodeWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean b = job.waitForCompletion(true);
		if (!b) {
			System.exit(2);
		}

		/// Call Job 2 where we perform HITS Algorithm to calculate Hub and Spoke
		/// Value at each Node in the graph iteratively.
		
		dijkstra(otherArgs[1], otherArgs[2]);

		//dijkstra("output1", "finaloutput");

	}

	public static void dijkstra(String input, String output) throws Exception {

		String temp = output;
		
		///  Run HITS Algorithm JOB:2 For 32 Times
		/// Setting the Value of k-> 32
		
		for (int i = 0; i < 32; i++) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "hubsandspokes");
			job.setJarByClass(HubsAndSpokes.class);
			job.setMapperClass(HubSpokeMapper.class);
			job.setReducerClass(HubSpokeReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NodeWritable.class);
			job.setOutputKeyClass(NodeWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			// Toggle the value of Input and Output variable 
			// For Next iteration
			input = output;
			output = temp + Integer.toString(i);
			
			// Wait for completing the JOB
			boolean b = job.waitForCompletion(true);
			if (!b)
				System.exit(2);
			// System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

	public static class HubSpokeMapper extends
			Mapper<Object, Text, Text, NodeWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			
			NodeWritable n = new NodeWritable(value.toString().trim());
			
			// Emit node to carry forward the Model.
			NodeWritable p = new NodeWritable(value.toString().trim());
			p.setIsNode(new Text("YES"));
			p.setIsInList(new Text("***"));
			context.write(new Text(p.getNid().toString()), p);
			
			// For Each OutLinks Emit This Node
			for (NodeWritable x : n.getOuts()) {
				if (!x.getNid().toString().equals(n.getNid().toString())) {
					n.setIsInList(new Text("YES"));
					n.setIsNode(new Text("NO"));
					context.write(new Text(x.getNid().toString()), n);
				}
			}

			
			// For Each Inlinks Emit This Node
			for (NodeWritable x : n.getIns()) {
				if (!x.getNid().toString().equals(n.getNid().toString())) {
					n.setIsInList(new Text("NO"));
					n.setIsNode(new Text("NO"));
					context.write(new Text(x.getNid().toString()), n);
				}
			}
		}
	}

	public static class HubSpokeReducer extends
			Reducer<Text, NodeWritable, NodeWritable, Text> {
		Map<Text, NodeWritable> map;

		
		// Setup Map for Reducer Lookup Table of Nodes
		// This Map stores the Updated Values of each Node
		public void setup(Context context) throws IOException,
				InterruptedException {
			map = new HashMap<Text, NodeWritable>();

		}

		// reduce(nid m, [d1,d2,...])
		public void reduce(Text key, Iterable<NodeWritable> values,
				Context context) throws IOException, InterruptedException {

			float spokeVal = 0.0f;
			float hubVal = 0.0f;
			ArrayList<NodeWritable> arr = new ArrayList<NodeWritable>();
			NodeWritable M = null;
			
			
			// Extract Original Node from Values
			for (NodeWritable x : values) {
				if (x.getNid().toString().equals(key.toString())) {
					M = new NodeWritable(x.toString());
					break;
				}
			}
			
			
			// Store All Values in arr
			for (NodeWritable x : values) {
				arr.add(x);
			}
			
			// Create List of All In Nodes
			ArrayList<NodeWritable> ins = new ArrayList<NodeWritable>();
			
			// Create List of all Out Nodes
			ArrayList<NodeWritable> outs = new ArrayList<NodeWritable>();

			// Populate Above Lists
			for (NodeWritable x : arr) {
				if (!x.getIsInList().toString().equals("***")) {
					if (x.getIsInList().toString().equals("YES")) {
						ins.add(x);
					} else {
						if (x.getIsInList().toString().equals("NO")) {
							outs.add(x);
						}
					}
				}
			}
			
			// For Each Out nodes change the current hubVal
			for (NodeWritable t : outs) {
				hubVal += Float.parseFloat(t.getSpokeVal().toString());

			}
			
			// For Each In Nodes change current spokeVal 
			for (NodeWritable t : ins) {
				spokeVal += (Float.parseFloat(t.getHubVal().toString()));
			}
			
			
			// Set new  Hub value 
			M.getHubVal().set(hubVal);
			
			// Set new Spoke Value
			M.getSpokeVal().set(spokeVal);
			
			// Store current Node in Map
			map.put(M.getNid(), M);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			float normHubVal = (float) 0.0;
			float normSpokeVal = (float) 0.0;

			// Calculate Sum of Squares of Each nodes Hub And spoke value
			for (Text nid : map.keySet()) {
				NodeWritable n = map.get(nid);
				normHubVal += Math.pow(n.getHubVal().get(), 2);
				normSpokeVal += Math.pow(n.getSpokeVal().get(), 2);
			}

			// Normalize the Hub And Spoke values and Emit for next iteration
			for (Text nid : map.keySet()) {
				NodeWritable n = map.get(nid);
				n.getHubVal().set(
						(float) (n.getHubVal().get() / Math.sqrt(normHubVal)));
				n.getSpokeVal().set(
						(float) (n.getSpokeVal().get() / Math
								.sqrt(normSpokeVal)));
				context.write(n, new Text());
			}
		}
	}
}
