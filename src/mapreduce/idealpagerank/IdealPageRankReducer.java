package mapreduce.idealpagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu Yu on 10/1/2015.
 * Received K-V pairs grouped by article title (3 cases)
 * Input Key: Text
 * ---> 1: Article_K
 * ---> 2: Article_I_K (Article K from article I)
 * ---> 3: Article_K
 * Input Value: Iterable<Text> :
 * ---> 1: "!" ( When there is article exists, in other words, this value will not exist if this article is red link )
 * ---> 2: "Article_K \t Page_rank_K \t Total_Articles_Count_In_Article_K" ( shuffle and grouped in from other K-V outputs in mapper )
 * ---> 3: "|Article_K_I1,Article_K_I2,...,Article_K_In"
 * Output Key: Text ---> Article_K
 * Output Value: Text ---> Page_rank_K \t Article_I1_K,Article_I2_K,...,Article_In_K
 */
public class IdealPageRankReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isRedLink = false;
    }
}
