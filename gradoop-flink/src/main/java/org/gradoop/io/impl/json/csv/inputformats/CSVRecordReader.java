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

package org.gradoop.io.impl.json.csv.inputformats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CSVRecordReader extends RecordReader<LongWritable, Text> {
  @Override
  public void initialize(InputSplit inputSplit,
    TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return false;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
