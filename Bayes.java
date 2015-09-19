import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.text.Document;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Bayes {
	//things need to be count
	public static enum FinalCounter
	{
		T_Documents,
		CCAT_WORDS,
		ECAT_WORDS,
		GCAT_WORDS,
		MCAT_WORDS,
		CCAT,
		ECAT,
		GCAT,
		MCAT,
		UNIQUE_WORDS,	
	}
	//init document
	public static class Document{
		String labels;
		String[] words;
		Vector<String> tokens;
		String[] DWords;
		public Document(String doc){
			int labelIndex = doc.indexOf("\t");
			labels=doc.substring(0,labelIndex);
			
			String context = doc.substring(labelIndex);
			String[] words = context.split("\\s+");
			tokens = new Vector<String>();
			for (int i = 0; i < words.length; i++) {
				words[i] = words[i].replaceAll("\\W", "");
				if (words[i].length() > 0) {
					tokens.add(words[i]);
				}
			}
			DWords = tokens.toArray(new String[tokens.size()]);
			
			//words=context.split("\\s+");
		}
	}
	
	public static class Map1 extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one= new IntWritable(1);
		public void map(Object key, Text text,Context context) throws IOException, InterruptedException{
			Document doc = new Document(text.toString());
			
			context.getCounter(FinalCounter.T_Documents).increment(1);
			
			if(doc.labels.contains("CCAT"))
				context.getCounter(FinalCounter.CCAT).increment(1);
			if(doc.labels.contains("ECAT"))
				context.getCounter(FinalCounter.ECAT).increment(1);
			if(doc.labels.contains("GCAT"))
				context.getCounter(FinalCounter.GCAT).increment(1);
			if(doc.labels.contains("MCAT"))
				context.getCounter(FinalCounter.MCAT).increment(1);
			
			for(String token:doc.DWords) {
				if(doc.labels.contains("CCAT")){
					context.getCounter(FinalCounter.CCAT_WORDS).increment(1);
					context.write(new Text("CCAT"+" "+ token), one);
				}
				if(doc.labels.contains("ECAT")){
					context.getCounter(FinalCounter.ECAT_WORDS).increment(1);
					context.write(new Text("ECAT"+" "+ token), one);
				}
				if(doc.labels.contains("GCAT")){
					context.getCounter(FinalCounter.GCAT_WORDS).increment(1);
					context.write(new Text("GCAT"+" "+ token), one);
				}
				if(doc.labels.contains("MCAT")){
					context.getCounter(FinalCounter.MCAT_WORDS).increment(1);
					context.write(new Text("MCAT"+" "+ token), one);
				}
			}
				
		}
	}
	//label words count
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		   
			public void reduce(Text key, Iterable<IntWritable> values, Context context) 
					throws IOException, InterruptedException {
					int sum = 0;
					for (IntWritable val : values) {				
						sum += val.get();	
					}
					context.write(key, new IntWritable(sum));
			}
	}
	//unique words
	public static class Map2 extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one= new IntWritable(1);
		public void map(Object key, Text text,Context context) throws IOException, InterruptedException{
			Document doc=new Document(text.toString());
			for(String token:doc.words) {
                context.write(new Text(token),one);
			}
		}
	}
	public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		   
		public void reduce(Text key, Iterable<IntWritable> values, Context context) {
			context.getCounter(FinalCounter.UNIQUE_WORDS).increment(1);
		}
	}
	public static class Cmap1 extends Mapper<Object,Text,Text,Text>
    {
        public final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String docId=key.toString();
            Document doc = new Document(value.toString());
            for(String token:doc.words)
            {
                    context.write(new Text("CCAT " + token), new Text("<labels>" + doc.labels+"<id>"+docId));
                    context.write(new Text("ECAT " + token), new Text("<labels>" + doc.labels+"<id>"+docId));
                    context.write(new Text("GCAT " + token), new Text("<labels>" + doc.labels+"<id>"+docId));
                    context.write(new Text("MCAT " + token), new Text("<labels>" + doc.labels+"<id>"+docId));
            }
        }
    }

	//a word in each label
	 public static class Map3 extends Mapper<Object,Text,Text,Text>
	    {
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	        {
	            String tokens[] = value.toString().split("\\s+");

	            String label = tokens[0];
	            String word = tokens[1];
	            String count = tokens[2];

	            context.write(new Text(label + " " + word), new Text("<cal>" + count));
	        }
	    }
	 //first part
	 public static class CReduce1 extends Reducer<Text,Text,Text,Text>{
	        public void reduce(Text InKey, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	        	String key=InKey.toString();
	        	String category="No";
	        	long XxYy=0;
	        	long XanyYy=0;
	        	double result=0.0000000;
	        	ArrayList<String> labels = new ArrayList<String>();
	        	for(Text val: values){
	        		String v=val.toString();
	        		if(v.contains("<cal>"))
	        		{
	        			String strCount=v.replaceAll("<count>", "");
	                    XxYy = Long.parseLong(strCount);
	        		} else{
	        			System.out.println(v);
	        		}
	        	}
	        	
	        	if(key.contains("CCAT")) {
	                category="CCAT";
	                XanyYy = context.getConfiguration().getLong("CCATWORDS",0);
	            }
	            if(key.contains("ECAT")) {
	                category="ECAT";
	                XanyYy = context.getConfiguration().getLong("CCATWORDS", 0);
	            }
	            if(key.contains("GCAT")) {
	                category="GCAT";
	                XanyYy = context.getConfiguration().getLong("CCATWORDS", 0);
	            }
	            if(key.contains("MCAT")) {
	                category="MCAT";
	                XanyYy = context.getConfiguration().getLong("CCATWORDS", 0);
	            }
	            for(String label: labels)
	            {
	      
	                result = (double)XxYy;
	                result+=1* (double)1/(double)context.getConfiguration().getLong("UNIQUEWORDS", 0);
	                result/= XanyYy+1;
	                result = Math.log(result);
	                context.write(new Text(label),new Text(category+" "+result));
	            }


	        }
	       
	 }
	 public static class Cmap2 extends Mapper<Object,Text,Text,Text>
	    {
	        public final static IntWritable one = new IntWritable(1);
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	        {
	            String[] input = value.toString().split("\\s+");
	            String id=input[0];
	            String label=input[1];
	            String logProb=input[2];
	            context.write(new Text(id+" "+label), new Text(logProb));
	        }
	    }

	 public static class CReduce2 extends Reducer<Text,Text,Text,Text>
	    {

	        @Override
	        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	        {
	            String tokens[] = key.toString().split(" ");
	            String labels = tokens[0];
	            String keyText=tokens[1];
	            String category="";
	            long yAny=context.getCounter(FinalCounter.T_Documents).getValue();
	            double qy=1/4;
	            long Yy=0;

	            if(keyText.contains("CCAT")) {
	                category="CCAT";
	                Yy = context.getConfiguration().getLong("CCAT",0);
	            }
	            if(keyText.contains("ECAT")) {
	                category="ECAT";
	                Yy = context.getConfiguration().getLong("ECAT",0);

	            }
	            if(keyText.contains("GCAT")) {
	                category="GCAT";
	                Yy = context.getConfiguration().getLong("GCAT",0);
	            }
	            if(keyText.contains("MCAT")) {
	                category="MCAT";
	                Yy = context.getConfiguration().getLong("MCAT",0);
	            }

	            double sum=0;
	            for(Text value: values)
	            {
	                    sum += Double.parseDouble(value.toString());
	            }

	            double eq2 = Yy+(1*qy);
	            eq2/=(double)(yAny+1);

	            eq2=Math.log(eq2);

	            double result = sum+eq2;
	            context.write(new Text(labels), new Text(category+" "+result));
	        }
	    }
	 public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        GenericOptionsParser parser = new GenericOptionsParser(conf,args);
	        String[] rArgs = parser.getRemainingArgs();
	        String s3Path = rArgs[2];

	        FileSystem fs = FileSystem.get(new URI(s3Path),conf);



	        int numReducers = Integer.parseInt(rArgs[3]);

	        Path input1 = new Path(rArgs[0]);
	        Path input2 = new Path(rArgs[1]);
	        Path output = new Path(s3Path+"output/");
	        Path tmp= new Path(s3Path+"tmp/");
	        Path tmp1= new Path(s3Path+"tmp1/");
	        Path tmp2= new Path(s3Path+"tmp2/");
	        Path tmp3= new Path(s3Path+"tmp3/");

	        //does most counting in dataset
	        Job job = Job.getInstance(conf, "bayes hadoop");
	        job.setNumReduceTasks(numReducers);
	        job.setJarByClass(Bayes.class);
	        job.setMapperClass(Map1.class);
	        job.setReducerClass(Reduce.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, input1);
	        FileOutputFormat.setOutputPath(job, tmp);
	        System.out.println("Job 1");
	        job.waitForCompletion(true);
	        
	        Job job2 = Job.getInstance(conf, "bayes hadoop2");
	        job2.setNumReduceTasks(numReducers);
	        job2.setJarByClass(Bayes.class);
	        job2.setMapperClass(Map2.class);
	        job2.setReducerClass(Reduce2.class);
	        FileOutputFormat.setOutputPath(job2, tmp);
	        job2.setOutputKeyClass(Text.class);
	        job2.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job2, input1);
	        FileOutputFormat.setOutputPath(job2, tmp1);
	        FileOutputFormat.setOutputPath(job2, tmp1);
	        System.out.println("Job 2");
	        job2.waitForCompletion(true);
	        
	        Counters counters = job.getCounters();
	        Counters counters2 = job2.getCounters();
	        org.apache.hadoop.mapreduce.Counter totalDocuments = counters.findCounter(FinalCounter.T_Documents);
	        org.apache.hadoop.mapreduce.Counter uniqueWords = counters2.findCounter(FinalCounter.UNIQUE_WORDS);

	        org.apache.hadoop.mapreduce.Counter ccat = counters.findCounter(FinalCounter.CCAT);
	        org.apache.hadoop.mapreduce.Counter ecat = counters.findCounter(FinalCounter.ECAT);
	        org.apache.hadoop.mapreduce.Counter gcat = counters.findCounter(FinalCounter.GCAT);
	        org.apache.hadoop.mapreduce.Counter mcat = counters.findCounter(FinalCounter.MCAT);
	    

	        org.apache.hadoop.mapreduce.Counter ccatwords = counters.findCounter(FinalCounter.CCAT_WORDS);
	        org.apache.hadoop.mapreduce.Counter ecatwords = counters.findCounter(FinalCounter.ECAT_WORDS);
	        org.apache.hadoop.mapreduce.Counter gcatwords = counters.findCounter(FinalCounter.GCAT_WORDS);
	        org.apache.hadoop.mapreduce.Counter mcatwords = counters.findCounter(FinalCounter.MCAT_WORDS);

	        conf.setLong("TOTALDOCUMENTS", totalDocuments.getValue());
	        conf.setLong("UNIQUEWORDS", uniqueWords.getValue());

	        conf.setLong("CCAT", ccat.getValue());
	        conf.setLong("ECAT", ecat.getValue());
	        conf.setLong("GCAT", gcat.getValue());
	        conf.setLong("MCAT", mcat.getValue());

	        conf.setLong("CCATWORDS", ccatwords.getValue());
	        conf.setLong("ECATWORDS", ecatwords.getValue());
	        conf.setLong("GCATWORDS", gcatwords.getValue());
	        conf.setLong("MCATWORDS", mcatwords.getValue());
	        
	        Job job3 = Job.getInstance(conf, "bayes hadoop3");
	        job3.setNumReduceTasks(numReducers);
	        job3.setJarByClass(Bayes.class);
	        job3.setReducerClass(CReduce1.class);
	        job3.setOutputKeyClass(Text.class);
	        job3.setOutputValueClass(Text.class);
	        MultipleInputs.addInputPath(job3, input2, Text.class, Cmap1.class);
	        FileOutputFormat.setOutputPath(job3,tmp2);
	        System.out.println("Starting Job 3");
	        job3.waitForCompletion(true);
	        
	        Job job4 = Job.getInstance(conf,"bayes hadoop4");
	        job4.setNumReduceTasks(numReducers);
	        job4.setJarByClass(Bayes.class);
	        job4.setMapperClass(Map3.class);
	        job4.setReducerClass(CReduce2.class);
	        job4.setOutputKeyClass(Text.class);
	        job4.setOutputValueClass(Text.class);
	        job4.setMapOutputKeyClass(Text.class);
	        job4.setMapOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job4,tmp2);
	        FileOutputFormat.setOutputPath(job4,tmp3);
	        System.out.println("Job 4");
	        job4.waitForCompletion(true);
	        
	        Job job5 = Job.getInstance(conf,"bayes hadoop5");
	        job5.setNumReduceTasks(numReducers);
	        job5.setJarByClass(Bayes.class);
	        job5.setMapperClass(Cmap2.class);
	        job5.setReducerClass(CReduce2.class);
	        job5.setOutputKeyClass(Text.class);
	        job5.setOutputValueClass(Text.class);
	        job5.setMapOutputKeyClass(Text.class);
	        job5.setMapOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job5,tmp3);
	        FileOutputFormat.setOutputPath(job5,output);
	        System.out.println("Job 5");
	        job5.waitForCompletion(true);
	        
	        
	        
	      //  ------------------------------------
	        String line;
	        String[] tokens;
	        FileStatus[] status = fs.listStatus(output);


	        BufferedReader breader=null;
	        int total = 0;
	        int totalCorrect = 0;
	        for(int i=0;i<status.length;i++)
	        {

	            //System.out.println(status[i].getPath());
	            breader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));

	            line = breader.readLine();
	            while (line != null)
	            {
	                total++;
	                System.out.println(line);
	                tokens = line.split("\\s+");
	                if (tokens[0].contains(tokens[1]))
	                    totalCorrect++;

	                line = breader.readLine();
	            }
	        }


	        double accuracy = totalCorrect/(double)total;
	        String results = "Percent correct: "+totalCorrect+"/"+total+" = "+(accuracy*100)+"%";
	        System.out.println(results);
	        breader.close();
	        fs.close();


	        System.exit(0);

	        
	 }
}
