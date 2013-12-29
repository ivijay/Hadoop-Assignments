package MaxNumber;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class MaxNumberMapper extends Mapper<LongWritable,Text,IntWritable,DoubleWritable>{

	public static enum CustomCounters {
		InvalidNum;
	};
	
	double max = Integer.MIN_VALUE;

	public void map(LongWritable Key,Text value,Context context) throws InterruptedException, IOException {
		
//		System.out.println("in mapper");
		String num = value.toString();
//		validate for int
//		if not int then increment counter
		if(isNumeric(num)) {
			Double d = Double.parseDouble(num);
			if(d > max) {
				max=d;
				context.write(new IntWritable(1),new DoubleWritable(max));
			}
			
			
		}
		else {
			context.getCounter(CustomCounters.InvalidNum).increment(1);
		}
	}

	public static boolean isNumeric(String str)  
	{  
	  try  
	  {  
	    @SuppressWarnings("unused")
		double d = Double.parseDouble(str);  
	  }  
	  catch(NumberFormatException nfe)  
	  {  
	    return false;  
	  }  
	  return true;  
	}
}
