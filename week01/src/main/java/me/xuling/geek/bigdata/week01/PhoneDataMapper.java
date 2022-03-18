package me.xuling.geek.bigdata.week01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author jack
 * @since 2022/3/9
 **/
public class PhoneDataMapper extends Mapper<LongWritable, Text, Text, PhoneData> {

    @Override
    protected void map(LongWritable keyIn, Text valueIn, Context context)
            throws IOException, InterruptedException {

        String data = valueIn.toString();

        //分词：按空格来分词
        String[] words = data.split("\t");
        String phoneNumer = words[1];
        Long upFlow = Long.parseLong(words[7]);
        Long downFlow = Long.parseLong(words[8]);

        context.write(new Text(phoneNumer), new PhoneData(phoneNumer, upFlow, downFlow));
    }
}
