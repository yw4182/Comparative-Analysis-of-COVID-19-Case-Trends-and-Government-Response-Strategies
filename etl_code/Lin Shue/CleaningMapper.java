import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
  
  private SimpleDateFormat dateFormat;
  
  @Override
  public void setup(Context context) {
    dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    if (fields.length != 4) {
      // Ignore records that don't have 4 fields
      return;
    }
    String dateStr = fields[0].replaceAll("-", "");
    if (dateStr.equals("-") || fields[2].equals("0.0") || fields[3].equals("0.0")) {
      // Ignore records that have "-" or 0.0 in 3rd or 4th column
      return;
    }
    try {
      Date date = dateFormat.parse(fields[0]);
      String outputKey = String.format("%tY%tm%td", date, date, date);
      String outputValue = String.format("%s,%s,%s", fields[1], fields[2], fields[3]);
      context.write(new Text(outputKey), new Text(outputValue));
    } catch (Exception e) {
      // Ignore records that can't be parsed as dates
      return;
    }
  }
}
