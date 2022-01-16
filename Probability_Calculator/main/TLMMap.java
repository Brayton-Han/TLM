package main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class TLMMap extends Mapper<LongWritable, Text, Text, MyType> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String str[] = line.split("\\s+");  //注意不能简单用“ ”，不然只会分离第一个空格

        HashMap<Text, MyType> map = new HashMap<Text, MyType>();

        for(int i=0; i<str.length-2; ++i) {
            StringBuilder s = new StringBuilder();
            s.append(str[i]).append(" ").append(str[i+1]);
            Text ab = new Text(s.toString());
            Text c = new Text(str[i+2]);

            if(!map.containsKey(ab)) {
                map.put(ab, new MyType());
            }

            int val = (map.get(ab).containsKey(c) ? ((IntWritable) map.get(ab).get(c)).get(): 0);
            map.get(ab).put(c, new IntWritable(val + 1));
        }

        for(Entry<Text, MyType> entry : map.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }

}