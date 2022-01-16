package main;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map.Entry;

public class TLMCombine extends Reducer<Text, MyType, Text, MyType> {

    public void reduce(Text key, Iterable<MyType> values, Context context) throws IOException, InterruptedException{
        MyType map = new MyType();

        for(MyType value : values) {
            for(Entry<Writable, Writable> entry : value.entrySet()) {
                Text k = (Text) entry.getKey();
                int val = (map.containsKey(k) ? ((IntWritable) map.get(k)).get(): 0);
                map.put(k, new IntWritable(val + ((IntWritable)entry.getValue()).get()));
            }
        }

        context.write(key, map);
    }

}
