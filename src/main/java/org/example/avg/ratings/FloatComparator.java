package org.example.avg.ratings;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

import java.nio.ByteBuffer;

public class FloatComparator extends WritableComparator {
    public FloatComparator() {

        super(FloatWritable.class);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2,
                       int s2, int l2) {
        Float v1 = ByteBuffer.wrap(b1, s1, l1).getFloat();
        Float v2 = ByteBuffer.wrap(b2, s2, l2).getFloat();
        return v1.compareTo(v2) * (-1);
    }
}
