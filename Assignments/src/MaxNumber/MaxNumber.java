package MaxNumber;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MaxNumber {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"MaxNumber Demo");
		
		job.setMapperClass(MaxNumberMapper.class);
		job.setReducerClass(MaxNumberReducer.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
//		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println("max number program started");
		job.waitForCompletion(true);
		

	}

}
