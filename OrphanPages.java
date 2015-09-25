import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;


// my code ???
import org.apache.hadoop.fs.FileSystem;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
	// my code
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Orphan Page");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(NullWritable.class);

	jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
	FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(OrphanPages.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// my code
                String curLine = value.toString();
		String[] parts = curLine.split(":");
                context.write(new IntWritable(Integer.parseInt(parts[0])), new IntWritable(0));
		StringTokenizer tokenizer = new StringTokenizer(parts[1]);

                while(tokenizer.hasMoreTokens()) {
                        String nextToken = tokenizer.nextToken();
                        context.write(new IntWritable(Integer.parseInt(nextToken.trim().toLowerCase())), new IntWritable(1));
                }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// my code
		int sum = 0;
		for(IntWritable val : values) {
			sum += val.get();
		}
		if(sum == 0) {
			context.write(key, NullWritable.get());
		}
        }
    }
}
