import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LyricCleanReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text song : values) {
            String lyric = song.toString();
            String[] parts = lyric.split("\t");

            if (parts.length < 4)
                return;

            String language = parts[3];

            // if (!language.equalsIgnoreCase("en")) {
            // context.write(new Text(""), new Text(lyric));
            // }
            context.write(NullWritable.get(), new Text(lyric));
            break;
        }
    }
}
