
package org.gradoop.flink.io.impl.tlf.inputformats;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * This input format is used to extract complete TLF graph strings from
 * distributed hdfs files.
 */
public class TLFInputFormat extends TextInputFormat {

  /**
   * Returns the actual file reader which handles the file split.
   *
   * @param split the split of the file containing all TLF content
   * @param context current task attempt context
   * @return the TLFRecordReader
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit
    split, TaskAttemptContext context) {
    try {
      return new TLFRecordReader((FileSplit) split, context
        .getConfiguration());
    } catch (IOException ioe) {
      System.err.println("Error while creating TLFRecordReader: " + ioe);
      return null;
    }
  }
}
