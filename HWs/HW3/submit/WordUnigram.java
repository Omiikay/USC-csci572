import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordUnigram {
   public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
   {
     private Text word = new Text();
     private Text docID = new Text();
     private String reg = "[^a-z\\s]+"; 
      
      public void map(Object  key, Text value, Context context) throws IOException, InterruptedException 
      {
         String[] doc = value.toString().split("\t", 2);
         docID.set(doc[0]);
         String text = doc[1].toLowerCase().replaceAll(reg, " "); 
        
         StringTokenizer itr = new StringTokenizer(text);
         while (itr.hasMoreTokens()) 
         {
            word.set(itr.nextToken());
            context.write(word, docID);
         }
         System.gc();
         TimeUnit.MILLISECONDS.sleep(500);
      }
   }
   
   public static class IndexReducer extends Reducer<Text,Text,Text,Text> 
   {
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
      {
        HashMap<String, Integer> hash = new HashMap<>();
        for (Text docID : values)
        {
          hash.put(docID.toString(), hash.getOrDefault(docID.toString(), 0) + 1);
        }
        String result = "";
        for (Object docId : hash.keySet()) {
            result += ((result.equals(""))?"":"\t") + docId.toString() + ":" + hash.get(docId);
        }
        context.write(key, new Text(result));
      }
   }
   
   public static void main(String[] args) throws Exception 
   {
      Job job = new Job();
      job.setJobName("Unigram");
     
      job.setJarByClass(WordUnigram.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setReducerClass(IndexReducer.class);
		
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}// WordUnigram

