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
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			// Local HashMap to aggregate count of words with
			// O(1) access for inserting and retrieval
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if (doesStartWithA(word.toString())
						|| doesStartWithB(word.toString())
						|| doesStartWithC(word.toString())
						|| doesStartWithD(word.toString())
						|| doesStartWithE(word.toString())) {
					// If map already contains the word, increment its count
					if (map.containsKey(word.toString())) {
						map.put(word.toString(), map.get(word.toString()) + 1);
					} else {
						// insert new word with count 1
						map.put(word.toString(), 1);
					}
				}
			}

			// After all the words are scanned
			// Emit all the words with their corresponding counts
			for (String word : map.keySet()) {
				context.write(new Text(word), new IntWritable(map.get(word)));
			}
		}
	}

	// Custom Partitioner which allocates the keys as follows
	public static class CustomePartitioner extends
			Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {

			// this is done to avoid performing mod with 0
			if (numReduceTasks == 0)
				return 0;
			// Return 0 the key starts with letter a or A
			if (doesStartWithA(key.toString())) {
				return 0 % numReduceTasks;
			}

			// Return 1 the key starts with letter b or B
			if (doesStartWithB(key.toString())) {
				return 1 % numReduceTasks;
			}

			// Return 2 the key starts with letter c or C
			if (doesStartWithC(key.toString())) {
				return 2 % numReduceTasks;
			}
			// Return 3 the key starts with letter d or D
			if (doesStartWithD(key.toString())) {
				return 3 % numReduceTasks;
			} else {
				// Return 4 the key starts with letter e or E
				return 4 % numReduceTasks;
			}

		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// Method checks if the given string starts with the letter a or A
	public static boolean doesStartWithA(String s) {
		boolean result = false;
		if (s.length() > 0) {
			char first = s.charAt(0);
			if (first == 'a' || first == 'A') {
				result = true;
			}
		}
		return result;
	}

	// Method checks if the given string starts with the letter b or B
	public static boolean doesStartWithB(String s) {
		boolean result = false;
		if (s.length() > 0) {
			char first = s.charAt(0);
			if (first == 'b' || first == 'B') {
				result = true;
			}
		}
		return result;
	}

	// Method checks if the given string starts with the letter c or C
	public static boolean doesStartWithC(String s) {
		boolean result = false;
		if (s.length() > 0) {
			char first = s.charAt(0);
			if (first == 'c' || first == 'C') {
				result = true;
			}
		}
		return result;
	}

	// Method checks if the given string starts with the letter d or D
	public static boolean doesStartWithD(String s) {
		boolean result = false;
		if (s.length() > 0) {
			char first = s.charAt(0);
			if (first == 'd' || first == 'D') {
				result = true;
			}
		}
		return result;
	}

	// Method checks if the given string starts with the letter e or E
	public static boolean doesStartWithE(String s) {
		boolean result = false;
		if (s.length() > 0) {
			char first = s.charAt(0);
			if (first == 'e' || first == 'E') {
				result = true;
			}
		}
		return result;
	}

	// Main Method
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setPartitionerClass(CustomePartitioner.class);
		job.setMapperClass(TokenizerMapper.class);
		// In the following statement the combiner is set
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
