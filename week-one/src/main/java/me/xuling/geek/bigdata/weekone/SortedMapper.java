package me.xuling.geek.bigdata.weekone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author jack
 * @since 2022/3/9
 **/
public class SortedMapper extends Mapper<LongWritable, Text, PhoneData, NullWritable> {
    @Override
    protected void map(LongWritable keyIn, Text valueIn, Context context)
            throws IOException, InterruptedException {

        String data = valueIn.toString();

        //分词：按空格来分词
        String[] words = data.split("\t");
        String phoneNumer = words[0];
        Long upFlow = Long.parseLong(words[1]);
        Long downFlow = Long.parseLong(words[2]);

        context.write(new PhoneData(phoneNumer, upFlow, downFlow), NullWritable.get());
    }
}
