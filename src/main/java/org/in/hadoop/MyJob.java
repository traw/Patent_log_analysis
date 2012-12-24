package org.in.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: rajdip
 * Date: 19/12/12
 * Time: 23:59
 * To change this template use File | Settings | File Templates.
 */
public class MyJob extends Configured implements Tool{

    public static class MapClass extends MapReduceBase
            implements Mapper<Text ,Text, Text, Text>{

        @Override
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            output.collect(value, key);

        }
    }

    public static class  Reduce extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String csv = "";
            while (values.hasNext()){
                if(csv.length() > 0) {
                    csv += ",";
                }
                csv += values.next().toString();
            }

            output.collect(key, new Text(csv));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf  = getConf();

        JobConf job = new JobConf(conf, MyJob.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("MyJob");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.set("key.value.separator.in.input.line", ",");

        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args) ;
        System.exit(res);
    }
}
