import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Tharuni_Samineni_Program_2{
    // Mapper produces tweet json object as key and value as 1
    public static class RowCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text keyOut = new Text("Total rows count: ");
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context){
        try { // generates key value pair
            context.write(keyOut, one);
        } 
        catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } 
        catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
    // RowCountReducer reduces the (tweet,1) and counts the number of unique lines
    public static class RowCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context){
        int count = 0;
        for (IntWritable val : values) {
            count += val.get(); //counting the number of rows
        }
        try {
            context.write(key, new IntWritable(count)); //writing the total count to the output
        } 
        catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } 
        catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}

    public static void main(String[] args) throws Exception {

    // creates a job and set Mapper and Reducer classes    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "line count");
    job.setJarByClass(Tharuni_Samineni_Program_2.class);
    job.setMapperClass(RowCountMapper.class);
    job.setCombinerClass(RowCountReducer.class);
    job.setReducerClass(RowCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // setting input and output paths for the given paths in command line
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}