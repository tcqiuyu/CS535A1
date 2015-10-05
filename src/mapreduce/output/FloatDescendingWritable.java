package mapreduce.output;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu Yu on 10/4/2015.
 */
public class FloatDescendingWritable extends FloatWritable {
    public static class DecreasingComparator extends Comparator{
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
//    public FloatDescendingWritable(float value) {
//        super(value);
//    }
//
//    public int compareTo(FloatDescendingWritable o) {
//        return -super.compareTo(o);
//    }

//    private Float value;
//
//    public FloatDescendingWritable(Float value) {
//        this.value = value;
//    }
//
//    @Override
//    public void write(DataOutput out) throws IOException {
//        out.writeFloat(value);
//    }
//
//    @Override
//    public void readFields(DataInput in) throws IOException {
//        this.value = in.readFloat();
//    }
//
//    public Float getValue() {
//        return value;
//    }
//
//    @Override
//    public int compareTo(FloatDescendingWritable o) {
//        if (!(o instanceof FloatDescendingWritable)) {
//            throw new ClassCastException();
//        }
//        Float otherVal = o.getValue();
//        return -value.compareTo(otherVal);
//    }


}
