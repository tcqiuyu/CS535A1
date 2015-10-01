package mapreduce.idealpagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu Yu on 10/1/2015.
 * Received K-V pairs grouped by article title
 * Input Key: Text ---> Article_K
 * Input Value: Iterable<Text> ( 3 cases ):
 * ---> 1. "!" ( When there is article exists, in other words, this value will not exist if this article is red link )
 * ---> 2: "Article_K \t Page_rank_K \t Total_Articles_Count_In_Article_K"
 * ---> 3: "|Article_I1_K,Article_I2_K,...,Article_In_K"
 * Output Key: Text ---> Article_K
 * Output Value: Text ---> Page_rank_K \t Article_I1_K,Article_I2_K,...,Article_In_K
 */
public class IdealPageRankReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isRedLink = false;
    }
}
