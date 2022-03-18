package me.xuling.geek.bigdata.week01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author jack
 * @since 2022/3/9
 **/
public class PhoneDataReduce extends Reducer<Text, PhoneData, Text, PhoneData> {

    @Override
    protected void reduce(Text keyIn, Iterable<PhoneData> valueIn, Context context)
            throws IOException, InterruptedException {

        Long upFlow = 0L;
        Long downFlow = 0L;

        for (PhoneData phoneData : valueIn) {
            upFlow += phoneData.getUpFlow();
            downFlow += phoneData.getDownFlow();
        }

        context.write(keyIn, new PhoneData(keyIn.toString(), upFlow, downFlow));
    }
}
