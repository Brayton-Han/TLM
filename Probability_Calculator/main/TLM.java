package main;
import WordCount.Wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TLM {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        //获取输入的参数
        String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2){
            System.err.println("必须输入读取文件路径和输出路径");
            System.exit(2);
        }

        Path inputPath = new Path(otherArgs[0]);
        //Path midPath = new Path(otherArgs[1]);
        Path outputPath = new Path(otherArgs[1]);

        // 删除旧的输出文件，防止数据混乱
        FileSystem fs =FileSystem.get(conf);
        /*
        if(fs.exists(midPath)){
            fs.delete(midPath, true);
        }
        */
        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // WordCount
        // long Size = Wordcount.main(conf, inputPath, midPath);
        long Size = 10351807;
        // System.out.println("Size = " + Size);
        // if(Size==0)
        //    System.exit(1);

        conf.setLong("num", Size);
        Job job = Job.getInstance(conf, "Trigram Language Model");
        job.setJarByClass(TLM.class);

        // 设置读入格式
        job.setInputFormatClass(TextInputFormat.class);

        //设置读取文件的路径
        FileInputFormat.addInputPath(job, inputPath);

        //设置mapreduce程序的输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        //设置实现了map combine reduce函数的类
        job.setMapperClass(TLMMap.class);
        job.setCombinerClass(TLMCombine.class);
        job.setReducerClass(TLMReduce.class);

        //设置reduce函数的key和value值
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyType.class);

        //设置reduce的数量，由于默认值为1所以不设置的话会运行得很慢
        job.setNumReduceTasks(256);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}