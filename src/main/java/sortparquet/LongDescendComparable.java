package sortparquet;

import org.apache.hadoop.io.LongWritable;

public class LongDescendComparable extends LongWritable {

    public LongDescendComparable() {
        super();
    }

    public LongDescendComparable(long value) {
        super(value);
    }

    @Override
    public int compareTo(LongWritable o) {
        return -1 * super.compareTo(o);
    }
}
