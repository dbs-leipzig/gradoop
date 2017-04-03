/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.tlf.inputformats;

import org.apache.commons.io.Charsets;
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

import java.io.IOException;

/**
 * TLFRecordReader class to read through a given TLF document to
 * output graph blocks as records which are specified by the start tag and
 * end tag.
 */
public class TLFRecordReader extends RecordReader<LongWritable, Text> {

  /**
   * The byte representation of the start tag which is 't'.
   */
  private static final byte[] START_TAG_BYTE = "t".getBytes(Charsets
    .UTF_8);

  /**
   * The byte representation of the end tag which is 't', in this case 't' is
   * not only the end tag but also the start tag of the next graph.
   */
  private static final byte[] END_TAG_BYTE = "t".getBytes(Charsets.UTF_8);

  /**
   * The start position of the split.
   */
  private final long start;

  /**
   * The end position of the split.
   */
  private final long end;

  /**
   * Input stream which reads the data from the split file.
   */
  private final FSDataInputStream fsin;

  /**
   * Output buffer which writes only needed content.
   */
  private final DataOutputBuffer buffer = new DataOutputBuffer();

  /**
   * The current key.
   */
  private LongWritable currentKey;

  /**
   * The current value.
   */
  private Text currentValue;

  /**
   * The length of the buffer data to be set to the value.
   */
  private int valueLength;

  /**
   * Constructor for the reader which handles TLF splits and
   * initializes the file input stream.
   *
   * @param split the split of the file containing all TLF content
   * @param conf the configuration of the task attempt context
   * @throws IOException
   */
  public TLFRecordReader(FileSplit split, Configuration conf) throws
    IOException {
    // open the file and seek to the start of the split
    start = split.getStart();
    end = start + split.getLength();
    Path file = split.getPath();
    FileSystem fs = file.getFileSystem(conf);
    fsin = fs.open(split.getPath());
    fsin.seek(start);
  }

  /**
   * Reads the next key/value pair from the input for processing.
   *
   * @param key the new key
   * @param value the new value
   * @return true if a key/value pair was found
   * @throws IOException
   */
  private boolean next(LongWritable key, Text value) throws IOException {
    if (fsin.getPos() < end && readUntilMatch(START_TAG_BYTE, false)) {
      try {
        buffer.write(START_TAG_BYTE);
        if (readUntilMatch(END_TAG_BYTE, true)) {
          key.set(fsin.getPos());
          if (fsin.getPos() != end) {
            //- end tag because it is the new start tag and shall not be added
            valueLength = buffer.getLength() - END_TAG_BYTE.length;
          } else {
            // in this case there is no new start tag
            valueLength = buffer.getLength();
          }
          //- end tag because it is the new start tag and shall not be added
          value.set(buffer.getData(), 0, valueLength);
          //set the buffer to position before end tag of old graph which is
          // start tag of the new one
          fsin.seek(fsin.getPos() - END_TAG_BYTE.length);
          return true;
        }
      } finally {
        buffer.reset();
      }
    }
    return false;
  }

  /**
   * Reads the split and searches for matches with given 'match byte array'.
   *
   * @param match the match byte to be found
   * @param withinBlock specifies if match is within the graph block
   * @return true if match was found or the end of file was reached, so
   * that the current block can be closed
   * @throws IOException
   */
  private boolean readUntilMatch(byte[] match, boolean withinBlock) throws
    IOException {
    int i = 0;
    while (true) {
      int b = fsin.read();
      // end of file:
      if (b == -1) {
        return true;
      }
      // save to buffer:
      if (withinBlock) {
        buffer.write(b);
      }
      // check if we are matching:
      if (b == match[i]) {
        i++;
        if (i >= match.length) {
          return true;
        }
      } else {
        i = 0;
      }
      // see if we have passed the stop point:
      if (!withinBlock && i == 0 && fsin.getPos() >= end) {
        return false;
      }
    }
  }

  /**
   * Closes open buffers
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    fsin.close();
    buffer.close();
  }

  /**
   * Returns the current process of input streaming.
   *
   * @return percentage of the completion
   * @throws IOException
   */
  @Override
  public float getProgress() throws IOException {
    return (fsin.getPos() - start) / (float) (end - start);
  }

  /**
   * Returns the current key.
   *
   * @return the current key.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public LongWritable getCurrentKey() throws IOException,
    InterruptedException {
    return currentKey;
  }

  /**
   * Returns the current value.
   *
   * @return the current value
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  /**
   * Called once for initialization.
   *
   * @param split the split of the file containing all TLF content
   * @param context current task attempt context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
  }

  /**
   * Reads the next kex/value pair from the input for processing.
   *
   * @return true if a key/value pair was found
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    currentKey = new LongWritable();
    currentValue = new Text();
    return next(currentKey, currentValue);
  }
}
