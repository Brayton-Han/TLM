package main;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map.Entry;

public class TLMReduce extends Reducer<Text, MyType, Text, MyType> {

    static long num;

    public void reduce(Text key, Iterable<MyType> values, Context context) throws IOException, InterruptedException{
        num = context.getConfiguration().getLong("num", 0);
        MyType map = new MyType();
        // long sum = 0;
        MapWritable sums = new MapWritable();

        for(MyType value : values){
            for(Entry<Writable, Writable> entry : value.entrySet()) {
                Text k = (Text) entry.getKey();
                double val = (map.containsKey(k) ? ((DoubleWritable) map.get(k)).get(): 0);
                map.put(k, new DoubleWritable(val + ((IntWritable) entry.getValue()).get()));
                // sum += ((IntWritable) entry.getValue()).get();
                int v = (sums.containsKey(k) ? ((IntWritable)sums.get(k)).get() : 0);
                sums.put(k, new IntWritable(v + ((IntWritable) entry.getValue()).get()));
            }
        }

        // long finalSum = sum;
        // map.replaceAll((k, v) -> new DoubleWritable((((DoubleWritable) v).get() + 1) / (finalSum + num)));
        map.replaceAll((k, v) -> new DoubleWritable((((DoubleWritable) v).get() + 1) / (((IntWritable)sums.get(k)).get() + num)));

        context.write(key, map);
    }

}
