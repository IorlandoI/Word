//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
import java.io.IOException;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.FileWriter;

public class WordCount {

    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split(" ");
            for (String data : mydata) {
            	data = data.replace("!", "");
            	data = data.replace("?", "");
            	data = data.replace(".", "");
            	data = data.replace("\"", "");
            	data = data.replace(",", "");
            	data = data.replace("=", "");
            	data = data.replace("_", "");
            	data = data.replace("[", "");
            	data = data.replace(";", "");
            	data = data.replace("\t", "");
            	data = data.replace(" ", "");
            	data = data.toUpperCase();
            	
            	if(data.equals("")) {
            		data = data;
            	}else {
            		word.set(data); // set word as each input keyword
                    context.write(word, one); // create a pair <keyword, 1>
            	}
            		

            }
        }
    }

    public static class Reduce
            extends Reducer<Text,IntWritable,Text,IntWritable> {

    	int i = 0;
    	int sort = 0;
    	String[] match = new String[10];
    	int[] val = new int[10];
    	
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            if(i < 10){
            	val[i] = sum;
            	match[i] = key.toString();
            	i++;
            }
            
            //This sorts the first full array
            if(i == 10 && sort == 0) {
            	int ind = 0;
            	int changes = 0;
            	while(ind != 9) {
            		
            		if(val[ind] < val[ind + 1]) {
            			String temps = match[ind + 1]; 
            			int temp = val[ind + 1];
            			val[ind + 1] = val[ind];
            			match[ind + 1] = match[ind];
            			val[ind] = temp;
            			match[ind] = temps;
            			changes++;
            		}
            		
            		ind++;
            		if(changes > 0 && ind == 9) {
            			ind = 0;
            			changes = 0;
            		}
            		
            	}
            	sort++;
            }
            
            
            //This finds larger sum values and moves them accordingly
            if(i == 10) {
            	int k = 0;
            	while(k<10) {
            		if(val[k] < sum) {
            			int p = 9;
            			while(k < p) {
            				val[p] = val[p-1];
            				match[p] = match[p-1];
            				p--;
            			}
            			val[k] = sum;
                		match[k] = key.toString();
                		break;
            		}
            		k++;
            	}
            }
            
            
        }
        
        @Override
        public void cleanup(Context context) throws IOException,
        	InterruptedException {
        		int imp = 0;
        		while(imp < 10) {
        			context.write(new Text(match[imp]), new IntWritable(val[imp]));
        			imp++;
        		}
        	}
        
    }
        
        
       
        
        
    
    
    
    
    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        // create a job with name "wordcount"
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(IntWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}