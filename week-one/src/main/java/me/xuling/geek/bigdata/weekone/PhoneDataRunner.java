package me.xuling.geek.bigdata.weekone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author jack
 * @since 2022/3/9
 **/
public class PhoneDataRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {


        Configuration conf = new Configuration();

        Path output1 = new Path(args[1]);
        Path output2 = new Path(args[2]);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output1)) {
            fs.delete(output1, true);
        }

        if(fs.exists(output2)) {
            fs.delete(output2, true);
        }

        Job job = Job.getInstance(conf, "map job");
        job.setJarByClass(PhoneDataRunner.class);

        job.setMapperClass(PhoneDataMapper.class);
        job.setReducerClass(PhoneDataReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneData.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Job sortJob = Job.getInstance(conf, "sort job");
        sortJob.setJarByClass(PhoneDataRunner.class);

        sortJob.setMapperClass(SortedMapper.class);
        sortJob.setReducerClass(SortedReduce.class);

        sortJob.setMapOutputKeyClass(PhoneData.class);
        sortJob.setMapOutputValueClass(NullWritable.class);

        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(PhoneData.class);

        FileInputFormat.addInputPath(sortJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[2]));
        sortJob.setNumReduceTasks(1);

        if (job.waitForCompletion(true)) {
            return sortJob.waitForCompletion(true)? 0 : 1;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PhoneDataRunner(), args);
        System.exit(res);
    }
}
