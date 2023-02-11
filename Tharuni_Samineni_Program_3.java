import java.io.IOException;
import org.apache.log4j.Logger;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class Tharuni_Samineni_Program_3 {
 // The mapper class takes text as key and value as 1
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
 
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String regex ="text\":\\\"(.*)\\\",\"place";                          // Using Regex to set a pattern
            Matcher matcher = Pattern.compile(regex).matcher(value.toString());  // Matching the pattern and converting value(JSON object) to string
            
            while (matcher.find()) {
                System.out.println(matcher.group(1));
                StringTokenizer itr = new StringTokenizer(matcher.group(1));
		StringTokenizer itr1 = new StringTokenizer(matcher.group(1));  //copying the tweet into itr1
		String x = " ";
		String initialToken = " ";                             
                
	// Iterates through all the tokens present in the tweet
		while(itr1.hasMoreTokens()) {             
		 String y = itr1.nextToken().toString();
			x = x + y.toLowerCase();
		}
                // Checking if either of the keywords are present in the tweet
		if(x.contains("covid"))
		{
		  initialToken = "Covid, ";
		}
		else if(x.contains("olympics"))
		{
		  initialToken = "olympics, ";
		}
		else if(x.contains("covid") && x.contains("olympics"))
		{
		  initialToken = "Covid & olympics, ";
		}
                else
                {
                  initialToken = "Random keyword, ";
                 }
                
	 // Iterates through all the tokens present in the tweet and Extracts the hashtags from the tweet 
		while (itr.hasMoreTokens()) {
 
                    String token = itr.nextToken();
                    token = token.toLowerCase();
		    String[] tokens = token.split(",");
		for(String s: tokens){
                    if (s.startsWith("#")) {
			String finalToken = initialToken.concat(s);         // concating the keyword with the hastag
			word.set(finalToken);
                        context.write(word, one);
			finalToken = " ";
                    }
		   }
	
		
                }
            }
 
        }
    }
    
    //The partitioner class makes a partition for each keyword respectively  
    public static class Examplepartitioner extends
	    Partitioner<Text, IntWritable> {
		
		String partitionkey;
		public int getPartition(Text key, IntWritable value, int numPartitions){
		
			String check = key.toString();            // Coverting the key into string
			String[] checkfin = check.split(",");     
			String str1 = "Covid";     
			String str2 = "olympics";
			String a = checkfin[0];
			if(a.equals(str1)){                      //if keyword is covid sends to reducer 1
			     return 0;
			}
			else if(a.equals(str2)){                // if keyword is olympics sends to reducer 2
			     return 1;
			}
			else {
			     System.err.println("Only 2 partitions are allowed");
			     return 0;
			}
			
		}
	}
     
    // The Reducer class counts the total number of tweets for each keyword, hashtag par and sums up the values 
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
 
        private Map<Text, IntWritable> countMap = new HashMap<>();
 
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
 
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
 
                        
            countMap.put(new Text(key), new IntWritable(sum));
        }
 
      // Used to output the top 10 hashtags in file
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
 
            Map<Text, IntWritable> sortedMap = sortByValues(countMap);
 
            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }
 
    // sorts the map values accoring to input order
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(
            Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(
                map.entrySet());
 
        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
 
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
 
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
 
        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
 
        return sortedMap;
    }
 
 // main() of the program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Tharuni_Samineni_Program_3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setPartitionerClass(Examplepartitioner.class);
	job.setNumReduceTasks(2);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}