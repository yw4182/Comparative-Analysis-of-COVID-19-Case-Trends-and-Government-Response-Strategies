import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StatsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final int THIRD_COL_IDX = 2;
    private static final int FOURTH_COL_IDX = 3;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] columns = line.split(",");

        // Skip header row
        if (key.get() == 0) {
            return;
        }

        // Filter out rows with 0.0 values in column 3 and 4
        if (columns.length > 3 && columns[THIRD_COL_IDX].equals("0.0")) {
            return;
        }
        if (columns.length > 4 && columns[FOURTH_COL_IDX].equals("0.0")) {
            return;
        }

        // Emit column 3 value
        if (columns.length > 2 && !columns[THIRD_COL_IDX].equals("")) {
            DoubleWritable col3Value = new DoubleWritable(Double.parseDouble(columns[THIRD_COL_IDX]));
            context.write(new Text("column3"), col3Value);
        }

        // Emit column 4 value
        if (columns.length > 3 && !columns[FOURTH_COL_IDX].equals("")) {
            DoubleWritable col4Value = new DoubleWritable(Double.parseDouble(columns[FOURTH_COL_IDX]));
            context.write(new Text("column4"), col4Value);
        }
    }
}
