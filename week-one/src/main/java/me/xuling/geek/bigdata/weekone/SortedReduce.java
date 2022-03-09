package me.xuling.geek.bigdata.weekone;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author jack
 * @since 2022/3/9
 **/
public class SortedReduce extends Reducer<PhoneData, NullWritable, Text, PhoneData> {

    @Override
    protected void reduce(PhoneData keyIn, Iterable<NullWritable> valueIn, Context context)
            throws IOException, InterruptedException {
        context.write(new Text(keyIn.getPhoneNumber()), keyIn);
    }
}
