import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.google.common.base.Charsets;
public class MultiLineJSONInputFormat extends TextInputFormat {

	public class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
		LineRecordReader linereader;
		LongWritable current_key;
		Text current_value;

		public MultiLineRecordReader(byte[] recordDelimiterBytes) {
			linereader = new LineRecordReader(recordDelimiterBytes);
		}

		@Override
		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			linereader.initialize(genericSplit, context);
		}

		private String jsonText = "";
		@Override
		public boolean nextKeyValue() throws IOException {
			/* do something better here to use linereader and
            set current_key and current_value with a multiline value */
			boolean res = linereader.nextKeyValue();
			current_key = linereader.getCurrentKey();
			current_value = linereader.getCurrentValue();

			if(current_value == null) return res;

			String cv = current_value.toString();

			if(cv.startsWith("{")) {
				jsonText = cv;
				nextKeyValue();
			}
			else if(!cv.endsWith("}")) {
				jsonText = jsonText + cv;
				nextKeyValue();
			}
			else { // ends with } 
				jsonText = jsonText + cv;
			}

			current_value = new Text(jsonText);

			return res;
		}

		@Override
		public float getProgress() throws IOException {
			return linereader.getProgress();
		}

		@Override
		public LongWritable getCurrentKey() {
			return current_key;
		}

		@Override
		public Text getCurrentValue() {
			return current_value;
		}
		@Override
		public synchronized void close() throws IOException {
			linereader.close();}
	}

	// shouldn't have to change below here

	@Override
	public RecordReader<LongWritable, Text> 
	createRecordReader(InputSplit split,
			TaskAttemptContext context) {
		// same as TextInputFormat constructor, except return MultiLineRecordReader
		String delimiter = context.getConfiguration().get(
				"textinputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		return new MultiLineRecordReader(recordDelimiterBytes);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		// let's not worry about where to split within a file
		return false;
	}
}