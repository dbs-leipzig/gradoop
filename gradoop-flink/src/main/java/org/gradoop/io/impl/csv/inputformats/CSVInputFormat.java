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

package org.gradoop.io.impl.csv.inputformats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.gradoop.io.impl.tlf.inputformats.TLFRecordReader;

import java.io.IOException;

/**
 * This input format is used to extract csv data from distributed hdfs files.
 */
public class CSVInputFormat extends FileInputFormat<LongWritable, String[]> {

  private String delimiter;

  public CSVInputFormat(String delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * Returns the actual file reader which handles the file split.
   *
   * @param split the split of the file containing all TLF content
   * @param context current task attempt context
   * @return the TLFRecordReader
   */
  @Override
  public RecordReader<LongWritable, String[]> createRecordReader(InputSplit
    split, TaskAttemptContext context) {
    try {
      return new CSVRecordReader((FileSplit) split, context
        .getConfiguration(), delimiter);
    } catch (IOException ioe) {
      System.err.println("Error while creating TLFRecordReader: " + ioe);
      return null;
    }
  }
}
