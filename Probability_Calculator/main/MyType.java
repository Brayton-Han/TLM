package main;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Iterator;

public class MyType extends MapWritable {

    // 自定义输出格式，key值用引号括起来，方便后续的json.loads
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("{");
        Iterator<Entry<Writable, Writable>> iter = entrySet().iterator();

        while(iter.hasNext()) {
            Entry<Writable, Writable> entry = iter.next();
            str.append("\"").append(entry.getKey()).append("\": ").append(entry.getValue());
            if(iter.hasNext()) str.append(", ");
        }

        str.append("}");
        return str.toString();
    }
}
