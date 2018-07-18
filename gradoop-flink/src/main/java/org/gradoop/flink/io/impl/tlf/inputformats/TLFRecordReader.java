/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.gradoop.flink.io.impl.tlf.TLFConstants;

import java.io.IOException;

/**
 * TLFRecordReader class to read through a given TLF document to
 * output graph blocks as records which are specified by the start tag and
 * end tag.
 */
public class TLFRecordReader extends RecordReader<LongWritable, Text> {

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
    if (fsin.getPos() < end &&
      readUntilMatch(TLFConstants.START_TAG.getBytes(Charsets.UTF_8), false)) {
      try {
        buffer.write(TLFConstants.START_TAG.getBytes(Charsets.UTF_8));
        if (readUntilMatch(TLFConstants.END_TAG.getBytes(Charsets.UTF_8), true)) {
          key.set(fsin.getPos());
          if (fsin.getPos() != end) {
            //- end tag because it is the new start tag and shall not be added
            valueLength = buffer.getLength() - TLFConstants.END_TAG.getBytes(Charsets.UTF_8).length;
          } else {
            // in this case there is no new start tag
            valueLength = buffer.getLength();
          }
          //- end tag because it is the new start tag and shall not be added
          value.set(buffer.getData(), 0, valueLength);
          //set the buffer to position before end tag of old graph which is
          // start tag of the new one
          fsin.seek(fsin.getPos() - TLFConstants.END_TAG.getBytes(Charsets.UTF_8).length);
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
