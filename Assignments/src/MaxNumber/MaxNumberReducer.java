package MaxNumber;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class MaxNumberReducer extends Reducer<IntWritable,DoubleWritable,DoubleWritable,IntWritable> {

	public static DoubleWritable max = new DoubleWritable(Integer.MIN_VALUE);
	Text var = new Text(" is Maximum value");
	
	public void reduce(IntWritable key,Iterable<DoubleWritable> values,Context context) throws IOException,InterruptedException {
	
		System.out.println("in reducer");
		for(DoubleWritable d: values) {
			if(d.compareTo(max) > 0) {
				max=d;
			}
		}
		
		
		context.write(max,new IntWritable(1));
	}

}
