package mapreduce.output;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu Yu on 10/4/2015.
 */
public class SortingReducer extends Reducer<FloatDescendingWritable, Text, FloatDescendingWritable, Text> {

    @Override
    protected void reduce(FloatDescendingWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
