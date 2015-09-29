package pageparser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Qiu on 9/27/15.
 */
public class PageParserMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String title = getTitle(value);
        String content = getContent(value);

        Set<String> links = extractLinkFromContent(content);

        Text titleText = new Text(title.replace(" ", "_"));

        for (String link : links) {

            context.write(titleText, new Text(link));
        }

    }

    private Set<String> extractLinkFromContent(String content) {
        Matcher matcher = linkPattern.matcher(content);

        Set<String> links = new HashSet<>();
        // find link in content
        while (matcher.find()) {
            String link = matcher.group();
            link = link.substring(2, link.length() - 2);
            link = link.split("|")[0];
            links.add(link);
        }
        return links;
    }

    private String getTitle(Text val) throws CharacterCodingException {
        int titleStartPos = val.find("<title>");
        int titleEndPos = val.find("</title>", titleStartPos);
        titleStartPos += 7; // plus <title>
        if (titleStartPos == -1 || titleEndPos == -1) {
            return "";
        }
        String title = Text.decode(val.getBytes(), titleStartPos, titleEndPos - titleStartPos);
        return title;
    }

    private String getContent(Text val) throws IOException {
        int contentStartPos = val.find("<text");
        contentStartPos = val.find(">", contentStartPos);
        contentStartPos += 1;
        int contentEndPos = val.find("</text>", contentStartPos);
        if (contentStartPos == -1 || contentEndPos == -1) {
            return "";
        }
        String content = Text.decode(val.getBytes(), contentStartPos, contentEndPos - contentStartPos);
        throw (new IOException(content));
//        return content;
    }


    private boolean validLink() {

        return true;
    }
}
