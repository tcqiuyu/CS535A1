package mapreduce.idealpagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu Yu on 10/1/2015.<br>
 * Input Key: LongWritable ---> Text offset<br>
 * Input Value: Text ---> "Article_K \t Page_rank_K \t Article_I1_K,Article_I2_K,...,Article_In_K"<br>
 * Output Key: Text <br>
 * ---> 1: "Article_K"<br>
 * ---> 2: "Article_I_K"<br>
 * ---> 3: "Article_K"<br>
 * Output Value: Text <br>
 * ---> 1: "!" ( for detecting and ignoring pages that have been deleted or does not exists, see "Wiki:Red Link" )<br>
 * ---> 2: "Article_K \t Page_rank_K \t Total_Articles_Count_In_Article_K" ( for page rank calculation )<br>
 * ---> 3: "|Article_I1_K,Article_I2_K,...,Article_In_K" ( saved for iterative use )<br>
 */
public class IdealPageRankMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        int firstTabIdx = value.find("\t");
        int secondTabIdx = value.find("\t", firstTabIdx + 1);

        String articleK = Text.decode(value.getBytes(), 0, firstTabIdx);
        String articleKWithPageRank = Text.decode(value.getBytes(), 0, secondTabIdx);

        // case 1
        context.write(new Text(articleK), new Text("!"));

        // ignore articles with no outgoing links
        if (secondTabIdx == -1) return;

        String outgoingArticles = Text.decode(value.getBytes(), secondTabIdx + 1, value.getLength() - secondTabIdx - 1); //Article_I1_K, Article_I2_K, ..., Article_In_K

        String[] articles = outgoingArticles.split(",");
        for (String outgoingArticle : articles) {
            String outputVal = articleKWithPageRank + "\t" + articles.length;
            // case 2
            context.write(new Text(outgoingArticle), new Text(outputVal));
        }

        // case 3
        context.write(new Text(articleK), new Text("|" + outgoingArticles));
    }
}