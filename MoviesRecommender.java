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

public class MoviesRecommender{
    public static class MoviesRecommenderMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
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
                userId = -1;
                movieId = -1;
                rating = -1.0;
             }
            if (rating >0.0){
                context.write(new IntWritable(movieId), new Text(linevalues[0]+ "," + linevalues[2]));
            }
            
        }
    }


    public static class MoviesRecommenderReducer extends Reducer<IntWritable, Text, IntWritable , IntWritable>{
        Map<Integer, Set<Integer>> permFriends = new TreeMap<Integer, Set<Integer>>(); // userId and all friends userId
        Map<Integer, Set<Integer>> likedMovies = new TreeMap<Integer, Set<Integer>>(); // userId and setOfMovies
        Map<Integer, Set<Integer>> recommendedMovies = new TreeMap<Integer, Set<Integer>>(); // userId and setOfMovies
        Map<Integer, Set<Integer>> watchedMovies = new TreeMap<Integer, Set<Integer>>(); //userId and watchedMovies
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Integer movieId = new Integer(key.get());
            Integer userId;
            Double rating;
            Vector<Integer> vecIterable = new Vector<Integer>();

            // fills vecIterable, likedMovies and watchedMovies
            for(Text value:values){
                String line = value.toString();
                String[] linevalues = line.split(",");
                userId = Integer.valueOf(linevalues[0]);
                rating = Double.valueOf(linevalues[1]);
                
                if (rating>=3.0){
                    vecIterable.add(userId);
                    if (!likedMovies.containsKey(userId)){
                        likedMovies.put(userId, new TreeSet<Integer>());
                    }
                    likedMovies.get(userId).add(movieId);
                }
                if (!watchedMovies.containsKey(userId)){
                    watchedMovies.put(userId, new TreeSet<Integer>());
                }
                watchedMovies.get(userId).add(movieId);
                
            }


            // finding friends (permutation)
            for (Integer value:vecIterable){
                Set<Integer> friends = new TreeSet<Integer>();
                for (Integer anothervalue: vecIterable){
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
     
            for (Map.Entry<Integer, Set<Integer>> entry: permFriends.entrySet()){
                Integer userId = entry.getKey();
                Set<Integer> allUserFriends = entry.getValue();
                for (Integer aFriend: allUserFriends){
                    if (!recommendedMovies.containsKey(aFriend)){
                        recommendedMovies.put(aFriend, new TreeSet<Integer>());
                    }
                    recommendedMovies.get(aFriend).addAll(likedMovies.get(userId));
                    
                }
            }


            for(Map.Entry<Integer,Set<Integer>> entry: recommendedMovies.entrySet()){
                Integer outkey = entry.getKey();
                Set<Integer> outvalues = entry.getValue();
                
                for (Integer value:outvalues){
                    if (!watchedMovies.get(outkey).contains(value)){
                        context.write(new IntWritable(outkey.intValue()), new IntWritable(value.intValue()));
                    }
                }
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MoviesRecommender");
        job.setJarByClass(MoviesRecommender.class);
        job.setMapperClass(MoviesRecommenderMapper.class);
        // job.setCombinerClass(TopFinderReducer.class);
        job.setReducerClass(MoviesRecommenderReducer.class);
        job.setMapOutputKeyClass(IntWritable.class); //mapper context.write part
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class); // reducer context.write part
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}