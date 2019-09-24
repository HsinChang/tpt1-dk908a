package tpt.dk908a;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.Writable;

import org.apache.log4j.Logger;


public class SelectPairs {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    Text key = new Text();
	    Text value = new Text();
	    String line = moviePair.toString();
	    if(line.charAt(0)!='u') 
	    	{
	        	String[] line_values = line.split("\\s+");
	        	String num = line_values[1];
	        	line_values = line_values[0].split(",");
	        	key.set(line_values[0]);
	        	value.set(line_values[1] + "," + num);
	        	context.write(key, value);
    		}
	    // use
	    //context.write(key,value)
	}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {	
	@Override
	public void reduce(Text movie, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
	    // FILL HERE
	    Text value = new Text();
	    int max = 0;
	    String rcm = null;
	    for(Text val : values){
	    	String vals = val.toString();
	    	String[] val_temp = vals.split(",");
	    	int num_temp = Integer.parseInt(val_temp[1]);
	    	String rcm_tmp = val_temp[0];
	    	if(max == 0 || num_temp > max) {
	    		max = num_temp;
	    		rcm = rcm_tmp;
	    	}
	    }
	    value.set(rcm);
	    context.write(movie, value);

	    // use
	    // context.write(key,value);
	}
    }
}


