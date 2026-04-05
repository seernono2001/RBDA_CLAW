import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YTReducer extends Reducer<Text, Text, Text, Text> {

  private long duplicateVideoCount = 0;
  private long uniqueVideoCount = 0;

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    String k = key.toString();
    if (k.endsWith("|value")) {
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      long sum = 0;
      long count = 0;
      for (Text val : values) {
        long v = Long.parseLong(val.toString());
        if (v < min)
          min = v;
        if (v > max)
          max = v;
        sum += v;
        count++;
      }
      String field = k.substring(0, k.lastIndexOf("|value"));
      context.write(new Text(field + " | count"), new Text(String.valueOf(count)));
      context.write(new Text(field + " | min"), new Text(String.valueOf(min)));
      context.write(new Text(field + " | max"), new Text(String.valueOf(max)));
      context.write(new Text(field + " | avg"), new Text(String.format("%.2f", (double) sum / count)));
    } else if (k.startsWith("video_id|") && !k.equals("video_id|null")) {
      long count = sumValues(values);
      uniqueVideoCount++;
      if (count > 1) {
        duplicateVideoCount++;
        // Uncomment the line below to list every duplicate id:
        // context.write(new Text("video_id | duplicate"), new Text(k.substring(9) + "
        // (x" + count + ")"));
      }
    } else {
      long total = sumValues(values);
      context.write(key, new Text(String.valueOf(total)));
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    context.write(new Text("video_id | unique_count"), new Text(String.valueOf(uniqueVideoCount)));
    context.write(new Text("video_id | duplicate_ids"), new Text(String.valueOf(duplicateVideoCount)));
  }

  private long sumValues(Iterable<Text> values) {
    long total = 0;
    for (Text val : values) {
      total += Long.parseLong(val.toString());
    }
    return total;
  }
}
