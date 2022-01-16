package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordcountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); //读取一行数据
        String str[] = line.split(" "); //使用“ ”分隔符将一行数据切成多个单词并存在数组中
        for(String s :str) { //将一个单词变成<key, value>形式
            context.write(new Text(s), new IntWritable(1));
        }
    }

}