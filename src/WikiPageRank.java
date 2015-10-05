import input.DumpInputFormat;
import mapreduce.output.FloatDescendingWritable;
import mapreduce.output.SortingMapper;
import mapreduce.output.SortingReducer;
import mapreduce.pageparser.PageParserMapper;
import mapreduce.pageparser.PageParserReducer;
import mapreduce.pagerank.IdealPageRankReducer;
import mapreduce.pagerank.PageRankMapper;
import mapreduce.pagerank.TaxPageRankReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 9/26/15.
 */
public class WikiPageRank {

    public static int MAX_ITER = 10;
    public static Path INPUT = new Path("/data/");
    public static String OUTPUT = "/home/output/";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        WikiPageRank wikiPageRank = new WikiPageRank();
        Path firstOutputPath = new Path(OUTPUT + "pagerank/parsedOutput");
//        wikiPageRank.runPageParsingJob(INPUT, firstOutputPath);
//
        String idealOutput = OUTPUT + "pagerank/ideal_iter";
//        wikiPageRank.runIdealPageRankJob(firstOutputPath, new Path(idealOutput + 1));
//        for (int iter = 1; iter < MAX_ITER; iter++) {
//            String inputPath = idealOutput + (iter);
//            String outputPath = idealOutput + (iter + 1);
//            wikiPageRank.runIdealPageRankJob(new Path(inputPath), new Path(outputPath));
//        }
        String idealSortedOutput = OUTPUT + "pagerank/ideal_sorted_output2/";
        wikiPageRank.runSortingJob(new Path(idealOutput + MAX_ITER), new Path(idealSortedOutput));

        String taxOutput = OUTPUT + "pagerank/tax_iter";
//        wikiPageRank.runTaxPageRankJob(firstOutputPath, new Path(taxOutput + 1));
//        for (int iter = 1; iter < MAX_ITER; iter++) {
//            String inputPath = taxOutput + (iter);
//            String outputPath = taxOutput + (iter + 1);
//            wikiPageRank.runTaxPageRankJob(new Path(inputPath), new Path(outputPath));
//        }
        String taxSortedOutput = OUTPUT + "pagerank/tax_sorted_output2/";
        wikiPageRank.runSortingJob(new Path(taxOutput + MAX_ITER), new Path(taxSortedOutput));
    }

    public void runPageParsingJob(Path inputPath, Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job pageParsing = Job.getInstance(configuration, "Page-Parsing");

        pageParsing.setJarByClass(WikiPageRank.class);

        pageParsing.setMapperClass(PageParserMapper.class);
        pageParsing.setReducerClass(PageParserReducer.class);

        FileInputFormat.setInputPaths(pageParsing, inputPath);
        pageParsing.setInputFormatClass(DumpInputFormat.class);

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }

        FileOutputFormat.setOutputPath(pageParsing, outputPath);
        pageParsing.setMapOutputKeyClass(Text.class);
        pageParsing.setMapOutputValueClass(Text.class);
        pageParsing.setOutputFormatClass(TextOutputFormat.class);
        pageParsing.setOutputKeyClass(Text.class);
        pageParsing.setOutputValueClass(Text.class);

        pageParsing.waitForCompletion(true);
    }

    public void runIdealPageRankJob(Path inputPath, Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job idealPageRank = Job.getInstance(configuration, "Ideal-PageRank");

        idealPageRank.setJarByClass(WikiPageRank.class);

        //set mapper/combiner/reducer
        idealPageRank.setMapperClass(PageRankMapper.class);
        idealPageRank.setReducerClass(IdealPageRankReducer.class);

        //set input path
        FileInputFormat.setInputPaths(idealPageRank, inputPath);
        FileInputFormat.setInputDirRecursive(idealPageRank, true);
        idealPageRank.setInputFormatClass(TextInputFormat.class);

        //set output path
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }
        FileOutputFormat.setOutputPath(idealPageRank, outputPath);
        idealPageRank.setMapOutputKeyClass(Text.class);
        idealPageRank.setMapOutputValueClass(Text.class);

        idealPageRank.setOutputFormatClass(TextOutputFormat.class);
        idealPageRank.setOutputKeyClass(Text.class);
        idealPageRank.setOutputValueClass(Text.class);

        idealPageRank.waitForCompletion(true);
    }

    public void runTaxPageRankJob(Path inputPath, Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job taxPageRank = Job.getInstance(configuration, "Tax-PageRank");

        taxPageRank.setJarByClass(WikiPageRank.class);

        //set mapper/combiner/reducer
        taxPageRank.setMapperClass(PageRankMapper.class);
        taxPageRank.setReducerClass(TaxPageRankReducer.class);

        //set input path
        FileInputFormat.setInputPaths(taxPageRank, inputPath);
        FileInputFormat.setInputDirRecursive(taxPageRank, true);
        taxPageRank.setInputFormatClass(TextInputFormat.class);

        //set output path
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }
        FileOutputFormat.setOutputPath(taxPageRank, outputPath);
        taxPageRank.setMapOutputKeyClass(Text.class);
        taxPageRank.setMapOutputValueClass(Text.class);

        taxPageRank.setOutputFormatClass(TextOutputFormat.class);
        taxPageRank.setOutputKeyClass(Text.class);
        taxPageRank.setOutputValueClass(Text.class);

        taxPageRank.waitForCompletion(true);
    }

    public void runSortingJob(Path inputPath, Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job sortingJob = Job.getInstance(configuration, "Sorting");

        sortingJob.setJarByClass(WikiPageRank.class);

        //set mapper/combiner/reducer
        sortingJob.setMapperClass(SortingMapper.class);
//        sortingJob.setReducerClass(SortingReducer.class);

        //set input path
        FileInputFormat.setInputPaths(sortingJob, inputPath);
        FileInputFormat.setInputDirRecursive(sortingJob, true);
        sortingJob.setInputFormatClass(TextInputFormat.class);

        //set output path
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }

        FileOutputFormat.setOutputPath(sortingJob, outputPath);

//        sortingJob.setMapOutputKeyClass(FloatDescendingWritable.class);
//        sortingJob.setMapOutputValueClass(Text.class);

        sortingJob.setOutputFormatClass(TextOutputFormat.class);
        sortingJob.setOutputKeyClass(FloatWritable.class);
        sortingJob.setOutputValueClass(Text.class);
        sortingJob.setSortComparatorClass(FloatDescendingWritable.DecreasingComparator.class);
        sortingJob.waitForCompletion(true);
    }

}
