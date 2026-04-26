import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LyricCleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.startsWith("title,"))
            return;

        List<String> song = getCSVLine(line);
        if (song.size() < 11) return;

        String artist = song.get(2).toLowerCase().trim().replaceAll("[^a-z0-9\\s]", "").replaceAll("\\s+", " ");
        String title = song.get(0).toLowerCase().replaceAll("[^a-z0-9\\s]", "").replaceAll("\\s+", " ").trim();
        String lyrics = song.get(6).replaceAll("<[^>]+>", "").replaceAll("&amp;", "&").replaceAll("\\s+", " ").trim();
        String language = song.get(10);
        String tag = song.get(1);
        String year = song.get(3);
        String views = song.get(4);

        if (lyrics.isEmpty() || lyrics.split("\\s+").length < 3) {
            return;
        }

        if (language.trim().length() == 0) {
            return;
        }



        String out = artist + " | " + title;
        String val = String.join("\t", artist, title, lyrics, language, tag, year, views);
        context.write(new Text(out), new Text(val));

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