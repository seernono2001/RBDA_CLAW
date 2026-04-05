import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YT {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: YT <input path> <output path>");
      System.exit(-1);
    }
 
    Job job = Job.getInstance();
    job.setJarByClass(YT.class);
    job.setJobName("YouTube Trending Data Profiling");
 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
    job.setMapperClass(YTMapper.class);
    job.setReducerClass(YTReducer.class);
 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
 
    job.setNumReduceTasks(1);
 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
