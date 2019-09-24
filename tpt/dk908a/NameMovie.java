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
import org.apache.commons.lang3.math.NumberUtils;

import org.apache.log4j.Logger;


public class NameMovie {
    
    public static class MapRatings extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    //General idea is to use the id of movie2 as key
	    	Text key = new Text();
    	    Text value = new Text();
	    	String line = moviePair.toString();
	    	String[] line_values = line.split("\\s+");//split movie1 and movie2
	        key.set(line_values[1]); //movie2
	        value.set(line_values[0]); //movie1
	        context.write(key,value);
	    // use
	    //context.write(key,value)
	}
    }

    public static class MapMovies extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    //Map key-movie_name pairs
	    Text key = new Text();
    	Text value = new Text();
	    String line = moviePair.toString();
	    if(line.charAt(0)!='m') 
	    {
	    	String[] line_values = line.split(",");
        	key.set(line_values[0]);
     	    value.set(line_values[1]);
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
	    Text name2 = new Text();
	    Text id1 = new Text();
	    for(Text valr : values){
	    	String val = valr.toString();
	    	if(NumberUtils.isParsable(val)) {
	    		id1.set(val+",");
	    	} else {
	    		name2.set(val);
	    	}
	    }
	    if(id1.getLength()>0){ //Some movies are never recommended, so we need to filter them 
	  	    context.write(id1, name2);
	  	}
	    // use
	    // context.write(key,value);
	}
    }

    public static class MapRatings2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    	Text key = new Text();
    	    Text value = new Text();
	    	String line = moviePair.toString();
	    	String[] line_values = line.split(",");
	        key.set(line_values[0]); //movie1
	        value.set(line_values[1]+",0"); //movie2, additional key to indicate which file this value is from
	        context.write(key,value);
	    // use
	    //context.write(key,value)
	}
    }

    public static class MapMovies2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text moviePair, Context context)
	    throws IOException, InterruptedException {
	    //FILL HERE
	    Text key = new Text();
    	Text value = new Text();
	    String line = moviePair.toString();
	    if(line.charAt(0)!='m') 
	    {
	    	String[] line_values = line.split(",");
        	key.set(line_values[0]);
     	    value.set(line_values[1]+",1"); //addtionnal key to indicate the source file
	        context.write(key, value);
    	}
	    // use
	    //context.write(key,value)
	}
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {	
	@Override
	public void reduce(Text movie, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
	    // FILL HERE
	    Text name2 = new Text();
	    Text id1 = new Text();
	    for(Text valr : values){
	    	String val = valr.toString();
	    	String[] vals = val.split(",");
	    	if(vals[1].equals("0")){ //if from item set
	    		name2.set(vals[0]);
	    	} else if (vals[1].equals("1")) { //if from movie name file
	    		id1.set(vals[0]+",");
	    	} else {
	    		continue;
	    	}
	    }
	    if(id1.getLength()>0&&name2.getLength()>0){
	  	    context.write(id1, name2);
	  	}
	    // use
	    // context.write(key,value);
	}
    }
}


