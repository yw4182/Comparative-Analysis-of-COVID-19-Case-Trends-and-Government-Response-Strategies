import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private Text cleanedRecord = new Text();
    private Set<Integer> uniformColumns = new HashSet<>();
    private int loadDateIndex = -1;

    @Override
    protected void setup(Context context) {
        // Add indices of columns with uniform values to the uniformColumns set
        // Replace the indices with the appropriate column indices from your dataset
        uniformColumns.add(3);
        uniformColumns.add(5);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        if (key.get() == 0) {
            // Find the index of the load_date column (replace 'load_date' with the correct column name in your dataset)
            for (int i = 0; i < fields.length; i++) {
                if (fields[i].equalsIgnoreCase("load_date")) {
                    loadDateIndex = i;
                    break;
                }
            }
            // Add load_date column index to the uniformColumns set
            uniformColumns.add(loadDateIndex);
        }

        // Remove the specified columns (load_date and uniform columns) from the output
        StringBuilder cleanedLine = new StringBuilder();
        int nullCount = 0;
        int validColumns = 0;
        int outputColumnIndex = 0;

        for (int i = 0; i < fields.length; i++) {
            if (!uniformColumns.contains(i)) {
                cleanedLine.append(fields[i]);

                if (outputColumnIndex < fields.length - uniformColumns.size() - 1) {
                    cleanedLine.append(",");
                }
                outputColumnIndex++;

                validColumns++;

                if (fields[i] == null || fields[i].trim().isEmpty()) {
                    nullCount++;
                }
            }
        }

        // Write header row
        if (key.get() == 0) {
            cleanedRecord.set(cleanedLine.toString());
            context.write(NullWritable.get(), cleanedRecord);
            return;
        }

        // Drop rows if they have null values for more than half of the feature columns
        if (nullCount <= validColumns / 2) {
            cleanedRecord.set(cleanedLine.toString());
            context.write(NullWritable.get(), cleanedRecord);
        }
    }
}
