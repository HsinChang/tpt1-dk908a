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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.lang.Integer;

import org.apache.log4j.Logger;


public class CountPairs {
    
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
	        	System.out.println(line_values[0]);
	        	line_values = line_values[1].split(",");
	        	for(int i = 0; i < line_values.length; i++){
	        		for(int j = i + 1; j < line_values.length; j++){
	        			key.set(line_values[i]+","+line_values[j]);
	        			value.set("1");
	        			context.write(key, value);
	        		}
	        	}
    		}
	    // use
	    //context.write(key,value)
	}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {	
	@Override
	public void reduce(Text movie, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
	    // FILL HERE
	    IntWritable result = new IntWritable();
      	int count=0;
      	for (Text val : values) {
      		count++;
      	}
      	result.set(count);
      	context.write(movie,result);
	    // use
	    // context.write(key,value);
	}
    }
}


