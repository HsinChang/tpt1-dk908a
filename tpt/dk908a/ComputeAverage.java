package tpt.dk908a;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;


public class ComputeAverage {
    
    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    	Text key = new Text();
    	    FloatWritable value = new FloatWritable();
	    	String line = moviePair.toString();
	    	if(line.charAt(0)!='u') 
	    	{
	        	String[] line_values = line.split(",");
	        	key.set(line_values[1]);
	        	value.set(Float.parseFloat(line_values[2]));
	        	context.write(key, value);
    		}
	    // use
	    //context.write(key,value)
	}
    }
    
    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void reduce(Text movie, Iterable<FloatWritable> values, Context context) 
	    throws IOException, InterruptedException {
	    // FILL HERE
	    //
	    	FloatWritable result = new FloatWritable();
	    	float sum=0;
      		int count=0;
      		for (FloatWritable val : values) {
        		sum += val.get();
        		count++;
      		}
      		result.set(sum/count);
      		context.write(movie, result);
	    // use
	    // context.write(key,value);
	    // for(FloatWritable v : values) { v.get(); /* for the value */}
	}
    }
}


