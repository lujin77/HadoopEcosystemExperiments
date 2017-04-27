package git.exps.bigdata;

import java.io.Serializable;
import java.sql.Array;

import org.apache.commons.lang.ArrayUtils;
import org.apache.crunch.*;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCount
 *
 * @author lujin
 * @date 16/10/9
 */
public class WordCount extends Configured implements Tool, Serializable {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println();
            System.err
                .println("Usage: " + this.getClass().getName() + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }
        //Pipeline pipeline = new MRPipeline(WordCount.class, getConf());
        Pipeline pipeline = MemPipeline.getInstance();
        PCollection<String> lines = pipeline.readTextFile(args[0]);

        PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
            public void process(String line, Emitter<String> emitter) {
                for (String word : line.split("\\s+")) {
                    emitter.emit(word);
                }
            }
        }, Writables.strings()); // Indicates the serialization format


        // the kind of data stored in the input PCollection.
        PTable<String, Long> counts = words.count();

        // Instruct the pipeline to write the resulting counts to a text file.
        pipeline.writeTextFile(counts, args[1]);
        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        String[] testArgs = new String[] {"../data/LICENSE.txt", "output"};
        int result = ToolRunner.run(new Configuration(), new WordCount(), testArgs);

        //int result = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(result);
    }
}
