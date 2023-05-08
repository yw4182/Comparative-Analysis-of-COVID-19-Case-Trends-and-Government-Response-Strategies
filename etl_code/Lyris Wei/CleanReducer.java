import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private Text result = new Text();
    private String separator = ",";

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            result.set(value.toString() + separator);
            context.write(NullWritable.get(), result);
        }
    }
}
