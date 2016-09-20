package wordcount;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class JSubmit {
    public static void main(String[] args) throws IOException,
            URISyntaxException, InterruptedException, ClassNotFoundException {

        /**

        //Path类为hadoop API定义，创建两个Path对象，一个输入文件的路径，一个输入结果的路径
        Path outPath = new Path("hdfs://localhost:9000/test");
        //输入文件的路径为本地linux系统的文件路径
        Path inPath = new Path("data/");
        //创建默认的Configuration对象
        Configuration conf = new Configuration();
        //根据地址和conf得到hadoop的文件系统独享
        //如果输入路径已经存在则删除
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        //根据conf创建一个新的Job对象，代表要提交的作业，作业名为JSubmit.class.getSimpleName()
        Job job = new Job(conf, JSubmit.class.getSimpleName());
        //1.1
        //FileInputFormat类设置要读取的文件路径
        FileInputFormat.setInputPaths(job, inPath);
        //setInputFormatClass设置读取文件时使用的格式化类
        job.setInputFormatClass(TextInputFormat.class);

        //1.2调用自定义的Mapper类的map方法进行操作
        //设置处理的Mapper类ls

        job.setMapperClass(JMapper.class);
        //设置Mapper类处理完毕之后输出的键值对 的 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //1.3分区，下面的两行代码写和没写都一样，默认的设置
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);
        //1.4排序，分组

        //1.5归约，这三步都有默认的设置，如果没有特殊的需求可以不管
        //2.1将数据传输到对应的Reducer

        //2.2使用自定义的Reducer类操作
        //设置Reducer类
        job.setReducerClass(JReducer.class);
        //设置Reducer处理完之后 输出的键值对 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //2.3将结果输出
        //FileOutputFormat设置输出的路径
        FileOutputFormat.setOutputPath(job, outPath);
        //setOutputFormatClass设置输出时的格式化类
        job.setOutputFormatClass(TextOutputFormat.class);

        //将当前的job对象提交
        job.waitForCompletion(true);
         */

        Configuration conf = new Configuration();
        // 多队列hadoop集群中，设置使用的队列
        conf.set("mapred.job.queue.name", "regular");
        // 之所以此处不直接用 argv[1] 这样的，是为了排除掉运行时的集群属性参数，例如队列参数，
        // 得到用户输入的纯参数，如路径信息等
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        for (String argsStr : otherArgs) {
            System.out.println("-->> " + argsStr);
        }
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(JSubmit.class);
        // map、reduce 输入输出类
        job.setMapperClass(JMapper.class);
        job.setCombinerClass(JReducer.class);
        job.setReducerClass(JReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        // 多子job的类中，可以保证各个子job串行执行
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}