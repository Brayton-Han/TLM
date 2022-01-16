package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Wordcount {

    public static long main(Configuration conf, Path inputPath, Path outputPath) throws Exception{
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(Wordcount.class);

        //设置读取文件的路径
        FileInputFormat.addInputPath(job, inputPath);

        //设置mapreduce程序的输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        //设置实现了map和reduce函数的类
        job.setMapperClass(WordcountMap.class);
        job.setReducerClass(WordcountReduce.class);

        //设置reduce函数的key和value值
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

}