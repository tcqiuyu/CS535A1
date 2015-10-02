package mapreduce.output;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu Yu on 10/2/2015.
 * Sorting Article by its page rank.
 * Input Key: LongWritable ---> Text offset
 * Input Value: Text ---> Article_K \t Page_rank_K \t Article_I1_K,Article_I2_K,...,Article_In_K<br>
 * Output Key: DoubleWritable ---> Page_rank_K<br>
 * Output Value: Text ---> Article_K<br>
 */
public class SortingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString();
        String[] split = val.split("\\t");
        String article_K = split[0];
        Double pagerank = Double.valueOf(split[1]);

        context.write(new DoubleWritable(pagerank), new Text(article_K));
    }
}
