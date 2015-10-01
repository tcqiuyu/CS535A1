package mapreduce.pageparser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu Yu on 9/29/2015.<br>
 * Input Key: Text ---> Article_K<br>
 * Input Value: Iterable<Text> ---> Article_I_K ( Article I from Article K )<br>
 * Output Key: Text ---> Article_K<br>
 * Output Value: Text ---> "\t 1.0 \t Article_I1_K,Article_I2_K,...,Article_In_K"<br>
 */

public class PageParserReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pagerank = "1.0\t";

        boolean first = true;
        for (Text link : values) {
            if(!first) pagerank += ",";

            pagerank += link.toString();
            first = false;
        }

        context.write(key, new Text(pagerank));
    }

}