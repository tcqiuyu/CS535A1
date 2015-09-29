package pageparser;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu Yu on 9/29/2015.
 */
public class PageParserReducer extends Reducer {

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }

}
