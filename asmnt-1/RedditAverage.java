import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private Text _key = new Text();
		private LongPairWritable pair = new LongPairWritable();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			String line = value.toString();
			ObjectMapper json_mapper = new ObjectMapper();
			JsonNode data = null;
			try {
				data = json_mapper.readValue(line, JsonNode.class);
			} catch (Exception e) {
				e.printStackTrace();
			}

			pair.set(1, data.get("score").longValue()); // 1 comment corresponds to 1 line
			_key.set(data.get("subreddit").textValue());
			context.write(_key, pair);
		}
	}


	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

		private LongPairWritable result = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long _score = 0;
			int _comments = 0;
			for (LongPairWritable val : values) {

				_comments += val.get_0();
				_score += val.get_1();

			}

			result.set(_comments, _score);
			context.write(key, result);
		}
	}

	public static class RedditReducaer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			double average = 0;
			double score_total = 0;
			int _comments = 0;
			for (LongPairWritable val : values) {
				_comments += val.get_0();
				score_total +=  val.get_1();
			}
			average = score_total / _comments;
			result.set(average);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit mapper");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(MultiLineJSONInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setCombinerClass(RedditCombiner.class);
		job.setReducerClass(RedditReducaer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongPairWritable.class);  // based on mapper
		job.setOutputFormatClass(TextOutputFormat.class);
		MultiLineJSONInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}



}