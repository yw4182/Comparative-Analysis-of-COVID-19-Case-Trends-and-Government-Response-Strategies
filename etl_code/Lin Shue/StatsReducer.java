import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;


public class StatsReducer extends Reducer<Text, DoubleWritable, Text, Text> {
   public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      List<Double> list = new ArrayList<Double>();
      double sum = 0.0;
      double count = 0.0;
      double mean = 0.0;
      double median = 0.0;
      double mode = 0.0;

      for (DoubleWritable value : values) {
         double v = value.get();
         list.add(v);
         sum += v;
         count += 1.0;
      }

      Collections.sort(list);
      int n = list.size();

      // Calculate mean
      if (count > 0) {
         mean = sum / count;
      }

      // Calculate median
      if (n > 0) {
         if (n % 2 == 0) {
            median = (list.get(n/2-1) + list.get(n/2)) / 2.0;
         } else {
            median = list.get(n/2);
         }
      }

      // Calculate mode
      Map<Double, Integer> map = new HashMap<Double, Integer>();
      for (double v : list) {
         if (map.containsKey(v)) {
            map.put(v, map.get(v) + 1);
         } else {
            map.put(v, 1);
         }
      }

      int maxCount = 0;
      for (int countValue : map.values()) {
         if (countValue > maxCount) {
            maxCount = countValue;
         }
      }

      List<Double> modeList = new ArrayList<Double>();
      for (Map.Entry<Double, Integer> entry : map.entrySet()) {
         if (entry.getValue() == maxCount) {
            modeList.add(entry.getKey());
         }
      }

      if (modeList.size() > 0) {
         mode = modeList.get(0);
      }

      // Output results
      Text result = new Text(String.format("mean: %f, median: %f, mode: %f", mean, median, mode));
      context.write(key, result);
   }
}
