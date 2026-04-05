import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YTMapper
    extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();

    if (line.startsWith("video_id")) {
      return;
    }
    List<String> fields = parseCSVLine(line);
    if (fields.size() < 16) {
      context.write(new Text("malformed_rows"), new Text("1"));
      return;
    }

    context.write(new Text("total_rows"), new Text("1"));

    emitNumeric(context, "views", fields.get(7).trim());
    emitNumeric(context, "likes", fields.get(8).trim());
    emitNumeric(context, "dislikes", fields.get(9).trim());
    emitNumeric(context, "comment_count", fields.get(10).trim());

    emitCategorical(context, "category_id", fields.get(4).trim());
    emitCategorical(context, "comments_disabled", fields.get(12).trim());
    emitCategorical(context, "ratings_disabled", fields.get(13).trim());
    emitCategorical(context, "video_error_or_removed", fields.get(14).trim());

    String vid = fields.get(0).trim();
    if (vid.isEmpty()) {
      context.write(new Text("video_id|null"), new Text("1"));
    } else {
      // emit the id itself as key; reducer counts how many times it appears
      context.write(new Text("video_id|" + vid), new Text("1"));
    }
  }

  private void emitNumeric(Context context, String field, String raw) throws IOException, InterruptedException {
    if (raw.isEmpty()) {
      context.write(new Text(field + "|null"), new Text("1"));
    } else {
      try {
        long v = Long.parseLong(raw);
        if (v < 0) {
          context.write(new Text(field + "|malformed"), new Text("1"));
        } else {
          context.write(new Text(field + "|value"), new Text(String.valueOf(v)));
        }
      } catch (NumberFormatException e) {
        context.write(new Text(field + "|malformed"), new Text("1"));
      }
    }
  }

  private void emitCategorical(Context context, String field, String raw)
      throws IOException, InterruptedException {
    String val = raw.isEmpty() ? "NULL" : raw.toUpperCase();
    context.write(new Text(field + "|" + val), new Text("1"));
  }

  private List<String> parseCSVLine(String line) {
    List<String> fields = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (c == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          sb.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == ',' && !inQuotes) {
        fields.add(sb.toString());
        sb = new StringBuilder();
      } else {
        sb.append(c);
      }
    }
    fields.add(sb.toString());
    return fields;
  }
}
