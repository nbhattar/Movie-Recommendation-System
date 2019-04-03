// Niraj Bhattarai
package org.apache.hadoop.ramapo;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendMovies{
    public static class FriendMoviesMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        // Last two arguments are the input of the reducer class
        int userId;
        double rating;
        int movieId;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] linevalues = line.split(",");
            try {
                userId = Integer.parseInt(linevalues[0]);
                rating = Double.valueOf(linevalues[2]);
                movieId = Integer.parseInt(linevalues[1]);
             }
             catch (NumberFormatException e)
             {
                userId = 0;
                movieId = 0;
                rating = 0.0;
             }
            if (rating >=3.0){
                context.write(new IntWritable(movieId), new IntWritable(userId));
            }

        }
    }


    public static class FriendMoviesReducer extends Reducer<IntWritable, IntWritable, IntWritable , IntWritable>{
        Map<Integer, Set<Integer>> permFriends = new TreeMap<Integer, Set<Integer>>();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{


            Vector<Integer> anothervalues = new Vector<Integer>();
            for (IntWritable value:values){
                anothervalues.add(new Integer(value.get()));

            }


            for (Integer value:anothervalues){
                Set<Integer> friends = new TreeSet<Integer>();
                for (Integer anothervalue: anothervalues){
                    friends.add(anothervalue);
                }
                if(!permFriends.containsKey(value)){
                    permFriends.put(value, friends);
                }
                else{
                    permFriends.get(value).addAll(friends);
                }
            }

    


        }
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for(Map.Entry<Integer,Set<Integer>> entry: permFriends.entrySet()){
                Integer outkey = entry.getKey();
                Set<Integer> outvalues = entry.getValue();
                for (Integer value:outvalues){
                    if (value>outkey){
                        context.write(new IntWritable(outkey.intValue()), new IntWritable(value.intValue()));
                    }
                }
            }

	    }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FriendMovies");
        job.setJarByClass(FriendMovies.class);
        job.setMapperClass(FriendMoviesMapper.class);
        // job.setCombinerClass(TopFinderReducer.class);
        job.setReducerClass(FriendMoviesReducer.class);
        job.setMapOutputKeyClass(IntWritable.class); //mapper context.write part
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class); // reducer context.write part
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
