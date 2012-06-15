package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private static String taggerFile = "/Users/nanand1/Downloads/stanford-postagger-2012-03-09/models/wsj-0-18-left3words.tagger";
    private Text word = new Text();
	private MaxentTagger tagger = null;
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	Path filePath = ((FileSplit) context.getInputSplit()).getPath();
    	if(tagger == null) {
    		try {
    			// initialize the tagger for noun extraction 
    			tagger = new MaxentTagger(taggerFile);
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		} catch (ClassNotFoundException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    	}
      
		try {
			// tag the string
			String whole = tagger.tagString(value.toString());
			// tokenize
			String [] parts = whole.split(" ");

			for (int i = 0; i < parts.length; i++) {
				
				// check if it has nouns
				if (parts[i].indexOf("NN") != -1) {
					// oh yes!! this one is noun.
					String[] thisword = parts[i].split("/");

					// collect if this noun is greater than 4 letters
					if (thisword[0].length() > 4) {
						word.set(filePath + "," + thisword[0].toLowerCase()+",");
						context.write(word, one);
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      if(sum > 10) {
    	  context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, new String[]{"input","output"}).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
