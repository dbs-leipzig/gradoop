package org.gradoop.io.graphgen;

import java.io.IOException;
import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Highly influenced by
 * 'https://github.com/apache/mahout/blob/b25a70a1bc6b9f8cb6c
 * 89947e0eaba5588463652/integration/src/main/java/org/apache/
 * mahout/text/wikipedia/XmlInputFormat.java'
 *
 * Created by stephan on 18.05.16.
 */
public class GraphGenInputFormat extends TextInputFormat{
  public static final byte[] START_TAG_BYTE = "t".getBytes(Charsets.UTF_8);;
  public static final byte[] END_TAG_BYTE = "t".getBytes(Charsets.UTF_8);;

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    try {
      return new GenGraphRecordReader((FileSplit) split, context.getConfiguration());
    } catch (IOException ioe) {
      System.err.println("Error while creating GraphGenRecordReader: " +  ioe);
      return null;
    }
  }

  /**
   * genGraphRecordReader class to read through a given GenGraph document to
   * output
   * graph blocks as records as specified
   * by the start tag and end tag
   *
   */
  public static class GenGraphRecordReader extends RecordReader<LongWritable, Text> {

    private final long start;
    private final long end;
    private final FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;

    public GenGraphRecordReader(FileSplit split, Configuration conf) throws IOException {
      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(conf);
      fsin = fs.open(split.getPath());
      fsin.seek(start);
    }

    private boolean next(LongWritable key, Text value) throws IOException {
      if (fsin.getPos() < end && readUntilMatch(START_TAG_BYTE, false)) {
        try {
          buffer.write(START_TAG_BYTE);
          if (readUntilMatch(END_TAG_BYTE, true)) {
            key.set(fsin.getPos());
            //vermutung hier minus endtagbyte
            value.set(buffer.getData(), 0, buffer.getLength());
            return true;
          }
        } finally {
          buffer.reset();
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      Closeables.close(fsin, true);
    }

    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        // end of file:
        if (b == -1) {
          return false;
        }
        // save to buffer:
        if (withinBlock) {
          buffer.write(b);
        }
        // check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length) {
            return true;
          }
        } else {
          i = 0;
        }
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos() >= end) {
          return false;
        }
      }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      currentKey = new LongWritable();
      currentValue = new Text();
      return next(currentKey, currentValue);
    }
  }
}
