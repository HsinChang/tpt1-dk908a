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
import java.util.List;
import java.util.ArrayList;


public class ExtractUserMovies {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    //
	    	Text key = new Text();
	    	Text movie = new Text();
	    	String line = moviePair.toString();
	    	if(line.charAt(0)!='u') 
	    	{
	        	String[] line_values = line.split(",");
	        	if(Float.parseFloat(line_values[2])>=3.5){
	        		key.set(line_values[0]);
	        		movie.set(line_values[1]);
	        		context.write(key, movie);
	        	}
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
	    //
	    	List<String> results = new ArrayList<String>();
	    	Text result = new Text();
	    	for (Text val : values) {
	    		results.add(val.toString());
	    	}
	    	result.set(String.join(",", results));
	    	context.write(movie, result);
	    // use
	    // context.write(key,value);
	}
    }
}


