package mapreduce.pagerank;

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
 * ---> 2: "Article_K \t Page_rank_K (1/n for the first iter) \t Total_Articles_Count_In_Article_K" ( shuffle and grouped in from other K-V outputs in mapper )
 * ---> 3: "|Article_K_I1,Article_K_I2,...,Article_K_In"
 * Output Key: Text ---> Article_K
 * Output Value: Text ---> Page_rank_K \t Article_I1_K,Article_I2_K,...,Article_In_K
 */
public class IdealPageRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (key.toString().equals("!")) {
            return;
        }

        boolean isRedLink = true;
        float pagerank = 0;
        String outgoingArticles = "";
        String inputVal;
        for (Text val : values) {
            inputVal = val.toString();

            // case 1
            if (inputVal.equals("!")) {
                isRedLink = false;
                continue;
            }

            // case 3
            if (inputVal.substring(1).equals("|")) {
                outgoingArticles = "\t" + inputVal.substring(1);
                continue;
            }

            // case 2
            String[] split = inputVal.split("\\t");
            Double pagerank_K;
            Integer article_count;
            try {
                pagerank_K = Double.valueOf(split[1]);
                article_count = Integer.valueOf(split[2]);
            } catch (ArrayIndexOutOfBoundsException e) {
                continue;
            }

            pagerank += (pagerank_K / article_count);
        }

        if (isRedLink) return;
        float finalPagerank = pagerank;

        context.write(key, new Text(finalPagerank + outgoingArticles));
    }
}
