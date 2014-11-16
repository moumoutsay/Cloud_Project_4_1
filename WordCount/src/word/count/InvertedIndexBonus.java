/** 
 * @author: Wei-Lin Tsai weilints@andrew.cmu.edu
 * @content: 
 *     For Cloud Compputing Project 4.1 
 * */
package word.count;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class InvertedIndexBonus {

	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		public static HashSet<String> stopList;  
		
		private void prepareStopList(Context context) {
			stopList = new HashSet<String>();
			try {
				Configuration conf =  context.getConfiguration();
				Path[] myPath = DistributedCache.getLocalCacheFiles(conf);
				BufferedReader reader = new BufferedReader(new FileReader(new File(myPath[0].getName())));
				//BufferedReader reader = new BufferedReader(new FileReader("/Users/moumoutsay/Documents/course/Cloud/Project4_1/stoplist"));
				
				String stopWord = null;
				while ((stopWord = reader.readLine()) != null) {
					stopList.add(stopWord);
					//ßSystem.out.println("get a stop: " + stopWord);
				}	
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get file Name
			FileSplit fs = (FileSplit) context.getInputSplit();
			String location = fs.getPath().getName();
			Text fileName = new Text(location);
			
			// prepare stoplist 
			prepareStopList(context);
			
			// Deal with input string
			String line = value.toString();
			// Replace punctuation with space
			line = line.replaceAll("[^A-Za-z0-9]", " ");
			line = line.toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreElements()) {
				word.set(tokenizer.nextToken());
				if (stopList.contains(word.toString())) {
					continue;
				}
				context.write(word, fileName);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			HashSet<String> fileNameHash = new HashSet<String>();
			
			// push to hash	
			for (Text val : values) {
				fileNameHash.add(val.toString());
			}
			String result =  new String(":");
			// doing output
			Iterator<String> myIter = fileNameHash.iterator();
			while (myIter.hasNext()) {
				String val = myIter.next();
				result = result + " " + val;
			}
			Text resultText = new Text(result);
			context.write(key, resultText);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// add cache 
		//DistributedCache.addCacheFile(new Path("hdfs://localhost:9000/config/stoplist").toUri(), conf);
		DistributedCache.addCacheFile(new Path("/config/stoplist").toUri(), conf);
	    //DistributedCache.addCacheFile(new File("/Users/moumoutsay/Documents/course/Cloud/Project4_1/stoplist").toURI(), conf);
		
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(InvertedIndexBonus.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
