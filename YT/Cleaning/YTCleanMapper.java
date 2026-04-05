import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YTCleanMapper extends Mapper<LongWritable, Text, Text, Text> {

  // Required columns for analysis — rows missing any of these are dropped
  // views=7, likes=8, dislikes=9, comment_count=10
  private static final int[] REQUIRED_NUMERIC_COLS = { 7, 8, 9, 10 };

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();

    // Skip header row
    if (line.startsWith("video_id"))
      return;

    List<String> fields = parseCSV(line);

    // Drop rows with too few columns (malformed CSV)
    if (fields.size() < 15) {
      context.getCounter("Cleaning", "dropped_malformed_csv").increment(1);
      return;
    }

    // Drop rows where video_id is missing
    String videoId = fields.get(0).trim();
    if (videoId.isEmpty()) {
      context.getCounter("Cleaning", "dropped_missing_video_id").increment(1);
      return;
    }

    // Drop rows where required numeric fields are null, non-numeric, or negative
    String[] numericNames = { "views", "likes", "dislikes", "comment_count" };
    boolean numericValid = true;
    long[] numericValues = new long[REQUIRED_NUMERIC_COLS.length];

    for (int i = 0; i < REQUIRED_NUMERIC_COLS.length; i++) {
      String raw = fields.get(REQUIRED_NUMERIC_COLS[i]).trim();
      if (raw.isEmpty()) {
        context.getCounter("Cleaning", "dropped_null_" + numericNames[i]).increment(1);
        numericValid = false;
        break;
      }
      try {
        long v = Long.parseLong(raw);
        if (v < 0) {
          context.getCounter("Cleaning", "dropped_negative_" + numericNames[i]).increment(1);
          numericValid = false;
          break;
        }
        numericValues[i] = v;
      } catch (NumberFormatException e) {
        context.getCounter("Cleaning", "dropped_malformed_" + numericNames[i]).increment(1);
        numericValid = false;
        break;
      }
    }
    if (!numericValid)
      return;

    // Normalize trending_date
    String trendingDate = normalizeTrendingDate(fields.get(1).trim());
    if (trendingDate == null) {
      context.getCounter("Cleaning", "dropped_bad_trending_date").increment(1);
      return;
    }

    // Normalize publish_time
    String publishTime = normalizePublishTime(fields.get(5).trim());

    // Normalize text fields trim whitespace, collapse
    String title = normalizeText(fields.get(2));
    String channelTitle = normalizeText(fields.get(3));

    // Normalize category_id
    String categoryId = fields.get(4).trim();
    if (categoryId.isEmpty()) {
      context.getCounter("Cleaning", "dropped_missing_category_id").increment(1);
      return;
    }

    String commentsDisabled = fields.get(12).trim().toUpperCase();

    String cleanRow = String.join("\t",
        videoId,
        trendingDate,
        title,
        channelTitle,
        categoryId,
        publishTime,
        String.valueOf(numericValues[0]), // views
        String.valueOf(numericValues[1]), // likes
        String.valueOf(numericValues[2]), // dislikes
        String.valueOf(numericValues[3]), // comment_count
        commentsDisabled);

    context.write(new Text(videoId), new Text(cleanRow));
  }

  private String normalizeTrendingDate(String raw) {
    if (raw.isEmpty())
      return null;
    String[] parts = raw.split("\\.");
    if (parts.length != 3)
      return null;
    try {
      int yy = Integer.parseInt(parts[0]);
      int dd = Integer.parseInt(parts[1]);
      int mm = Integer.parseInt(parts[2]);
      if (yy < 0 || dd < 1 || dd > 31 || mm < 1 || mm > 12)
        return null;
      int yyyy = 2000 + yy;
      return String.format("%04d-%02d-%02d", yyyy, mm, dd);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private String normalizePublishTime(String raw) {
    if (raw.isEmpty())
      return "";
    int tIdx = raw.indexOf('T');
    return tIdx > 0 ? raw.substring(0, tIdx) : raw;
  }

  private String normalizeText(String raw) {
    return raw.trim().replaceAll("\\s+", " ").toLowerCase();
  }

  private List<String> parseCSV(String line) {
    List<String> fields = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          current.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == ',' && !inQuotes) {
        fields.add(current.toString());
        current = new StringBuilder();
      } else {
        current.append(c);
      }
    }
    fields.add(current.toString());
    return fields;
  }
}
