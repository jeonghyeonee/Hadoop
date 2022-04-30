import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritable implements Writable {
    public int cnt;
    public double avg;
    public double sum;
    public double val;
    public double max = Integer.MIN_VALUE;
    public double min = Integer.MAX_VALUE;
    public boolean flag;


    @Override
    public void write(DataOutput out) throws IOException {
        
        out.writeInt(cnt);
        out.writeDouble(avg);
        out.writeDouble(sum);
        out.writeDouble(val);
        out.writeDouble(max);
        out.writeDouble(min);
        
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        cnt = in.readInt();
        avg = in.readDouble();
        sum = in.readDouble();
        val = in.readDouble();
        max = in.readDouble();
        min = in.readDouble();
        
    }

    public void set(double avg) {
        this.avg = avg;
//        this.flag = true;
    }

    public double get() {
        return val;
    }
}
