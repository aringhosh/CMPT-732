package asignment1;

//adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

//package ca.sfu.whatever;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Locale;
import java.text.BreakIterator;
import java.text.Normalizer;
import java.text.Normalizer.Form;

public class WordCountImproved extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{



		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		Locale locale = new Locale("en", "ca");
		BreakIterator breakiter = BreakIterator.getWordInstance(locale);

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			int start = breakiter.first();
			String line = value.toString();
			breakiter.setText(line);

			for (int end = breakiter.next();end != BreakIterator.DONE; start = end, end = breakiter.next()) { 
				String word1 = line.substring(start,end).trim();
				if ( word1.length() > 0 ) {
					word.set(Normalizer.normalize(word1, Form.NFD));
					context.write(word, one);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountImproved.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}