import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by Qiu on 9/26/15.
 */
public class Main {

    public static int max_iter = 10;
    public static Path inputPath = new Path("/data/");

    public static void main(String[] args) {

        Configuration configuration = new Configuration();

        //store an variable of total books count
        Job IdealPageRank = Job.getInstance(configuration, "Ideal-PageRank");

        //set main class
        IdealPageRank.setJarByClass(RecommendationSystem.class);

        //set mapper/combiner/reducer
        IdealPageRank.setMapperClass(TFIDFMapper.class);
        IdealPageRank.setReducerClass(TFIDFReducer.class);

        //set the combine file size to maximum 64MB
        IdealPageRank.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));
        IdealPageRank.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);

        //set input path
        FileInputFormat.setInputPaths(IdealPageRank, new Path(args[args.length - 2]));
        FileInputFormat.setInputDirRecursive(IdealPageRank, true);
        IdealPageRank.setInputFormatClass(CombineBooksInputFormat.class);

        //set output path
        FileOutputFormat.setOutputPath(IdealPageRank, firstTempPath);

        IdealPageRank.setMapOutputKeyClass(Text.class);
        IdealPageRank.setMapOutputValueClass(TextArrayWritable.class);

        IdealPageRank.setOutputFormatClass(TextOutputFormat.class);
        IdealPageRank.setOutputKeyClass(Text.class);
        IdealPageRank.setOutputValueClass(TextArrayWritable.class);

        IdealPageRank.waitForCompletion(true);
    }

}
