package com.projectfriendrec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Friend recommendation driver class.
public class FriendRec {
	public static void main(String[] args) throws Exception {
		// System.out.println("HELLO");
		// Long start = System.currentTimeMillis();
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "friend recommendation");
	    job.setJarByClass(FriendRec.class);
	    job.setMapperClass(FriendRecMapper.class);
	    // job.setCombinerClass(FriendRecReducer.class);
	    job.setReducerClass(FriendRecReducer.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(FriendRecordWritable.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    // job.setInputFormatClass(TextInputFormat.class);
	    // job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
