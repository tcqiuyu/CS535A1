import input.DumpInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import mapreduce.pageparser.PageParserMapper;
import mapreduce.pageparser.PageParserReducer;

import java.io.IOException;

/**
 * Created by Qiu on 9/26/15.
 */
public class WikiPageRank {

    public static int max_iter = 10;
    public static Path inputPath = new Path("/data/");
    public static Path outputPath = new Path("/home/output/");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        //store an variable of total books count
        Job IdealPageRank = Job.getInstance(configuration, "Ideal-PageRank");

        //set main class
        IdealPageRank.setJarByClass(WikiPageRank.class);

        //set mapper/combiner/reducer
        IdealPageRank.setMapperClass(PageParserMapper.class);
        IdealPageRank.setReducerClass(PageParserReducer.class);

        //set input path
        FileInputFormat.setInputPaths(IdealPageRank, inputPath);
        FileInputFormat.setInputDirRecursive(IdealPageRank, true);
        IdealPageRank.setInputFormatClass(DumpInputFormat.class);

        //set output path
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }
        FileOutputFormat.setOutputPath(IdealPageRank, outputPath);
        IdealPageRank.setMapOutputKeyClass(Text.class);
        IdealPageRank.setMapOutputValueClass(Text.class);

        IdealPageRank.setOutputFormatClass(TextOutputFormat.class);
        IdealPageRank.setOutputKeyClass(Text.class);
        IdealPageRank.setOutputValueClass(Text.class);

        IdealPageRank.waitForCompletion(true);
    }

}
