import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LyricProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.startsWith("title,"))
            return;

        List<String> song = getCSVLine(line);
     if (song.size() < 11) {
            context.write(new Text("BAD_SIZE_" + song.size()), new IntWritable(1));
            context.write(new Text("row_issue"), new IntWritable(1));
            return;
        }

        String title = song.get(0).trim();
        String artist = song.get(2).trim();
        String lyrics = song.get(6).trim();
        String language = song.get(10).trim();

        context.write(new Text("total_song"), new IntWritable(1));

        if (lyrics.isEmpty()) {
            context.write(new Text("missing_lyrics"), new IntWritable(1));
        } else {
            int words = lyrics.split("\\s+").length;
            if (words < 3) {
                context.write(new Text("short_lyrics"), new IntWritable(1));
            }
            context.write(new Text("lyric_word_sum"), new IntWritable(words));
            context.write(new Text("lyric_count"), new IntWritable(1));
        }

        String normalArtist = artist.toLowerCase().replaceAll("[^a-z0-9\\s]", "").replaceAll("\\s+", " ").trim();
        context.write(new Text("artist:" + artist), new IntWritable(1));
        context.write(new Text("artist_filter:" + normalArtist), new IntWritable(1));
        context.write(new Text("title:" + title), new IntWritable(1));
        if (!language.equalsIgnoreCase("en")) {
            context.write(new Text("not_english"), new IntWritable(1));
        } else {
            context.write(new Text("english"), new IntWritable(1));
        }

        if (language.contains(",")) {
            context.write(new Text("many_language"), new IntWritable(1));
        }
    }

    public List<String> getCSVLine(String line) {
        List<String> lyricList = new ArrayList<>();
        StringBuilder lyricCSV = new StringBuilder();
        boolean hasQuote = false;

        int i = 0;
        char[] chars = line.toCharArray();

        while (i < chars.length) {
            char c = chars[i];

            if (c == '"') {
                if (hasQuote && i + 1 < chars.length && chars[i + 1] == '"') {
                    lyricCSV.append('"');
                    i += 2;
                    continue;
                } else {
                    hasQuote = !hasQuote;
                }

            } else if (c == ',' && !hasQuote) {
                lyricList.add(lyricCSV.toString().trim());
                lyricCSV = new StringBuilder();

            } else {
                lyricCSV.append(c);
            }

            i++;
        }

        lyricList.add(lyricCSV.toString().trim());
        return lyricList;
    }
}