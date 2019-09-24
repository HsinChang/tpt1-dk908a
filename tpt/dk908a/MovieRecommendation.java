package tpt.dk908a;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.Integer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapred.JobClient;


public class MovieRecommendation extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MovieRecommendation.class);
    private static boolean USER_MOVIES = false;
    private static boolean COUNT_PAIRS = false;
    private static boolean SELECT_PAIRS = false;
    private static boolean NAME_MOVIE1 = false;
    private static boolean NAME_MOVIE2 = false;

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new MovieRecommendation(), args);
	System.exit(res);
    }


    public int run(String[] args) throws Exception {
	// FILL HERE
    	int ctemp = Integer.parseInt(args[2]);
    	if(ctemp == 1) {
    		this.USER_MOVIES = true;
    	} else if(ctemp == 2) {
    		this.COUNT_PAIRS = true;
    	} else if(ctemp == 3) {
    		this.SELECT_PAIRS = true;
    	} else if(ctemp == 4 ){
    		this.NAME_MOVIE1 = true;
    	} else if(ctemp == 5){
    		this.NAME_MOVIE2 = true;
    	} else {
    		System.out.println("Invalid arguments" + args[2]);
    		return 0;
    	}
    	if(MovieRecommendation.USER_MOVIES){
    		Job job = Job.getInstance(getConf(), "Movies");
			job.setJarByClass(this.getClass());
			FileInputFormat.addInputPath(job, new Path(args[0]+"/ratings.csv"));
			FileOutputFormat.setOutputPath(job, new Path(args[1]+".t1"));
			job.setMapperClass(ExtractUserMovies.Map.class);
			job.setReducerClass(ExtractUserMovies.Reduce.class);
			job.setOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			this.USER_MOVIES = false;
			return job.waitForCompletion(true) ? 0 : 1 ;
		} else if (MovieRecommendation.COUNT_PAIRS) {

			Job job1 = new Job(getConf());
			FileInputFormat.addInputPath(job1, new Path(args[0]+".t1"));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]+".t2"));
			job1.setJarByClass(this.getClass());
			job1.setMapperClass(CountPairs.Map.class);
			job1.setReducerClass(CountPairs.Reduce.class);
			job1.setOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);
			this.COUNT_PAIRS = false;
			return job1.waitForCompletion(true) ? 0 : 1 ;

		} else if (MovieRecommendation.SELECT_PAIRS) {
			Job job2 = new Job(getConf());
			FileInputFormat.addInputPath(job2, new Path(args[0]+".t2"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+".t3"));
			job2.setJarByClass(this.getClass());
			job2.setMapperClass(SelectPairs.Map.class);
			job2.setReducerClass(SelectPairs.Reduce.class);
			job2.setOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			this.SELECT_PAIRS = false;
			return job2.waitForCompletion(true) ? 0 : 1 ;
		} else if(MovieRecommendation.NAME_MOVIE1) {
			Job job3 = new Job(getConf());
			job3.setJarByClass(this.getClass());
			MultipleInputs.addInputPath(job3, new Path("/datasets/movie_small/movies.csv"), TextInputFormat.class, NameMovie.MapMovies.class);
			MultipleInputs.addInputPath(job3, new Path(args[0]+".t3"), TextInputFormat.class, NameMovie.MapRatings.class);
			FileOutputFormat.setOutputPath(job3, new Path(args[1]+".t4"));
			job3.setReducerClass(NameMovie.Reduce.class);
			job3.setOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			this.NAME_MOVIE1 = false;
			return job3.waitForCompletion(true) ? 0 : 1;
		} else if (MovieRecommendation.NAME_MOVIE2){
			Job job4 = new Job(getConf());
			job4.setJarByClass(this.getClass());
			MultipleInputs.addInputPath(job4, new Path("/datasets/movie_small/movies.csv"), TextInputFormat.class, NameMovie.MapMovies2.class);
			MultipleInputs.addInputPath(job4, new Path(args[0]+".t4"), TextInputFormat.class, NameMovie.MapRatings2.class);
			FileOutputFormat.setOutputPath(job4, new Path(args[1]+".t5"));
			job4.setReducerClass(NameMovie.Reduce2.class);
			job4.setOutputValueClass(Text.class);
			job4.setOutputKeyClass(Text.class);
			this.NAME_MOVIE2 = false;
			return job4.waitForCompletion(true) ? 0 : 1;

		} else {
			return 0;
		}

    }    
	    
}
