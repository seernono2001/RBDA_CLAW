import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class LyricCleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, Double> lexicon;

    @Override
    protected void setup(Context context) throws IOException {

        lexicon = new HashMap<>();

        InputStream is = LyricCleanMapper.class
                .getResourceAsStream("/vader_lexicon.txt");

        if (is == null) {
            System.out.println("Lex File not found");
            return;
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;

        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty())
                continue;

            String[] p = line.split("\\s+");

            try {
                String word = p[0].toLowerCase();
                double score = Double.parseDouble(p[1]);
                lexicon.put(word, score);
            } catch (Exception ignored) {
            }
        }

        br.close();

        System.out.println("LEX SIZE = " + lexicon.size());
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.startsWith("title,"))
            return;

        List<String> song = getCSVLine(line);
        if (song.size() < 11)
            return;

        String artist = song.get(2).toLowerCase().trim()
                .replaceAll("[^a-z0-9\\s]", "")
                .replaceAll("\\s+", " ");

        String title = song.get(0).toLowerCase()
                .replaceAll("[^a-z0-9\\s]", "")
                .replaceAll("\\s+", " ")
                .trim();

        String lyrics = song.get(6)
                .replaceAll("<[^>]+>", "")
                .replaceAll("&amp;", "&")
                .replaceAll("\\s+", " ")
                .toLowerCase()
                .trim();

        String language = song.get(10);
        String tag = song.get(1);
        String year = song.get(3);
        String views = song.get(4);

        if (lyrics.isEmpty() || lyrics.split("\\s+").length < 3)
            return;
        if (title.isEmpty())
            return;
        if (language.trim().length() == 0)
            return;

        double score = 0.0;
        String[] tokens = lyrics.split("\\s+");

        for (String t : tokens) {
            Double v = lexicon.get(t);
            if (v != null)
                score += v;
        }

        score = score / Math.sqrt(tokens.length + 1);

        String out = artist + " | " + title;

        String val = String.join("\t",
                artist,
                title,
                lyrics,
                language,
                tag,
                year,
                views,
                String.valueOf(score));

        context.write(new Text(out), new Text(val));
    }

    public List<String> getCSVLine(String line) {
        List<String> lyricList = new ArrayList<>();
        StringBuilder lyricCSV = new StringBuilder();

        boolean inQuotes = false;
        boolean inBraces = false;

        char[] chars = line.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];

            if (c == '"') {
                if (inQuotes && i + 1 < chars.length && chars[i + 1] == '"') {
                    lyricCSV.append('"');
                    i++;
                    continue;
                }
                inQuotes = !inQuotes;
                lyricCSV.append(c);
                continue;
            }

            if (c == '{')
                inBraces = true;
            if (c == '}')
                inBraces = false;

            if (c == ',' && !inQuotes && !inBraces) {
                lyricList.add(lyricCSV.toString().trim());
                lyricCSV = new StringBuilder();
                continue;
            }

            lyricCSV.append(c);
        }

        lyricList.add(lyricCSV.toString().trim());
        return lyricList;
    }
}