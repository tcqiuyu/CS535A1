package input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Qiu on 9/27/15.
 * This record reader reads through the wiki dump xml file.
 * It will read from {@code <page>} tag and end at {@code </page>} tag
 *
 * How the file is splited in {@link DumpInputFormat} does not matter here:
 * If a page is within multiple splits, the record reader which starts at
 * start tag will read across these splits, while the record reader starts
 * at later splits will anything before {@code </page>} tag because the missing of
 * {@code <page>} tag.
 */
public class PageRecordReader extends RecordReader<LongWritable, Text> {

    public static final String START_TAG = "<page>";
    public static final String END_TAG = "</page>";
    private long start;
    private long end;
    private FSDataInputStream fsDataInputStream;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private LongWritable key = new LongWritable();
    private Text value = new Text();

    private byte[] startTag = START_TAG.getBytes();
    private byte[] endTag = END_TAG.getBytes();

    public PageRecordReader(FileSplit split, TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        start = split.getStart();
        end = start + split.getLength();
        Path file = split.getPath();
        FileSystem fileSystem = file.getFileSystem(conf);
        fsDataInputStream = fileSystem.open(file);
        fsDataInputStream.seek(start);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    /**
     * Read starts until reaches <i>START_TAG</i> and ends until reaches <i>END_TAG</i>.
     * Key is the offset of start tag.
     * Value is the content between <page></page> tags.
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (fsDataInputStream.getPos() < end) {
            if (readUntilTag(startTag, false)) {
                try {
                    buffer.write(startTag);
                    if (readUntilTag(endTag, true)) {
                        key.set(fsDataInputStream.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (fsDataInputStream.getPos() - start) / (float) (end - start);
    }

    @Override
    public void close() throws IOException {
        fsDataInputStream.close();
    }

    /**
     * This method makes {@link FSDataInputStream} read continuously until meet the start/end tag;
     *
     * @param tag
     * @param inBlock
     * @return
     */
    private boolean readUntilTag(byte[] tag, boolean inBlock) throws IOException {
        int i = 0;
        while (true) {
            int b = fsDataInputStream.read();
            // end of file
            if (b == -1) return false;
            // if passed start tag, then write the section between start tag and end tag into buffer
            if (inBlock) buffer.write(b);

            // tag check
            if (b == tag[i]) {
                i++;
                if (i >= tag.length) return true;
            } else i = 0;

            if (!inBlock && i == 0 && fsDataInputStream.getPos() >= end) return false;
        }
    }
}
