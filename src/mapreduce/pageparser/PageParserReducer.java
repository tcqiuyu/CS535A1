package mapreduce.pageparser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu Yu on 9/29/2015.<br>
 * Input Key: Text ---> Article_K<br>
 * Input Value: Iterable<Text> ---> Article_K_I ( Article I from Article K )<br>
 * Output Key: Text ---> Article_K<br>
 * Output Value: Text ---> "\t 1.0 \t Article_K_I1,Article_K_I2,...,Article_K_In"<br>
 */

public class PageParserReducer extends Reducer<Text, Text, Text, Text> {

    private int n = 0;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.toString().equals("!")) {
            for (Text val : values) {
                n += Integer.valueOf(val.toString());
            }
        }


        String pagerank = String.valueOf(1 / n) + "\t";

        boolean first = true;
        for (Text link : values) {
            if (!first) pagerank += ",";

            pagerank += link.toString();
            first = false;
        }

        context.write(key, new Text(pagerank));
    }

}