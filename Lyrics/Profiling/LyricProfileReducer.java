import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LyricProfileReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String item = key.toString();
        if (item.startsWith("artist:") || item.startsWith("artist_filter:") || item.startsWith("title:")) {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            context.write(key, new Text(String.valueOf(count)));
        } else if (item.equals("lyric_word_sum")) {
            int sum = 0;
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (IntWritable val : values) {
                sum += val.get();
                min = Math.min(min, val.get());
                max = Math.max(max, val.get());
            }
            context.write(new Text("lyric_min"), new Text(String.valueOf(min)));
            context.write(new Text("lyric_max"), new Text(String.valueOf(max)));
            context.write(key, new Text(String.valueOf(sum)));
        } else {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }
}
