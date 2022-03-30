import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class Calculator extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Calculator(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        String inputPath = strings[0];
        String outputPath = strings[0] + ".out";

        Job job = Job.getInstance(getConf(), "word counting");
        job.setJarByClass(Calculator.class);

        job.setMapperClass(CalMapper.class);
        job.setReducerClass(CalReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

        return 0;
    }

    public static class CalMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        Text ok = new Text();
        DoubleWritable ov = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), ",");

            st.nextToken();
            String station_code = st.nextToken();
            String item_code = st.nextToken();

            ok.set(station_code+"\t"+item_code);

            double item_value = Double.parseDouble(st.nextToken());

            ov.set(item_value);

            context.write(ok, ov);

        }
    }

    public static class CalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        DoubleWritable ov = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            double sum=0;
            int cnt=0;

            for(DoubleWritable d : values){
                sum += d.get();
                cnt += 1;
            }

            double avg = sum/cnt;

            context.write(key, ov);
        }
    }


}