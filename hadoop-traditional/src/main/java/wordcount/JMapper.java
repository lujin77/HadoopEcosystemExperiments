package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//自定义的Mapper类必须继承Mapper类，并重写map方法实现自己的逻辑
public class JMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //处理输入文件的每一行都会调用一次map方法，文件有多少行就会调用多少次
    protected void map(
            LongWritable key,
            Text value,
            org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws java.io.IOException, InterruptedException {
        //key为每一行的起始偏移量
        //value为每一行的内容

        //每一行的内容分割，如hello   world，分割成一个String数组有两个数据，分别是hello，world
        String[] ss = value.toString().toString().split("\t");
        //循环数组，将其中的每个数据当做输出的键，值为1，表示这个键出现一次
        for (String s : ss) {
            //context.write方法可以将map得到的键值对输出
            context.write(new Text(s), new LongWritable(1));
        }
    };
}