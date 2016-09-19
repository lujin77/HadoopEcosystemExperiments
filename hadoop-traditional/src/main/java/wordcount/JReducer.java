package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//自定义的Reducer类必须继承Reducer，并重写reduce方法实现自己的逻辑，泛型参数分别为输入的键类型，值类型；输出的键类型，值类型；之后的reduce类似
public class JReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    //处理每一个键值对都会调用一次reduce方法，有多少个键值对就调用多少次
    protected void reduce(
            Text key,
            java.lang.Iterable<LongWritable> value,
            org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>.Context context)
            throws java.io.IOException, InterruptedException {
        //key为每一个单独的单词，如：hello，world，you，me等
        //value为这个单词在文本中出现的次数集合，如{1,1,1}，表示总共出现了三次
        long sum = 0;
        //循环value，将其中的值相加，得到总次数
        for (LongWritable v : value) {
            sum += v.get();
        }
        //context.write输入新的键值对（结果）
        context.write(key, new LongWritable(sum));
    };
}