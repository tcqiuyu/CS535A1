package mapreduce.pageparser;

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
 * Created by Qiu on 9/27/15.<br>
 * Input Key: LongWritable ---> Text Offset<br>
 * Input Value: Text ---> Content of each article<br>
 * Output Key: Text ---> Article_K<br>
 * Output Value: Text ---> Article_K_I ( Article I from Article K )<br>
 */
public class PageParserMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
    private int n = 0;

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
        n++;

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("!"), new Text(String.valueOf(n)));
    }

    private Set<String> extractLinkFromContent(String content) throws IOException {
        Matcher matcher = linkPattern.matcher(content);

        Set<String> links = new HashSet<>();
        // find link in content
        while (matcher.find()) {
            String link = matcher.group();
            int endPos = link.indexOf("]");

            int barPos = link.indexOf("|");
            if (barPos > 0) {
                endPos = barPos;
            }

            // ignore the page anchor
            int poundPos = link.indexOf("#");
            if (poundPos > 0) {
                endPos = poundPos;
            }

            link = link.substring(2, endPos);

            if (!isValidLink(link)) {
                continue;
            }

            if (link.contains("&amp;")) {
                link = link.replace("&amp;", "&");
            }

            link = link.replaceAll("\\s", "_");
            link = link.replaceAll(",", "");
            links.add(link);
        }
        return links;
    }

    private boolean isValidLink(String link) {
        if (link.startsWith("#")) return false;
        if (link.startsWith(",")) return false;
        if (link.startsWith(".")) return false;
        if (link.startsWith("\'")) return false;
        if (link.startsWith("-")) return false;
        if (link.startsWith("{")) return false;

        if (link.contains(":")) return false;
        if (link.contains(",")) return false;
        return true;
    }

    private String linkValidation(String link) {

        return link;
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

    private String getContent(Text val) throws CharacterCodingException {
        int contentStartPos = val.find("<text");
        contentStartPos = val.find(">", contentStartPos);
        contentStartPos += 1;
        int contentEndPos = val.find("</text>", contentStartPos);
        if (contentStartPos == -1 || contentEndPos == -1) {
            return "";
        }
        String content = Text.decode(val.getBytes(), contentStartPos, contentEndPos - contentStartPos);

        return content;
    }
}
