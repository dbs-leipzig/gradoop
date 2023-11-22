/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.parquet.common;

import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormatBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;

/**
 * OutputFormat implementation allowing to use Hadoop (mapreduce) OutputFormats which don't provide a key
 * with Flink.
 *
 * @param <T> Value Type
 */
public class HadoopValueOutputFormat<T> extends HadoopOutputFormatBase<Void, T, T> {

  /**
   * Creates a new Flink output format.
   *
   * @param mapreduceOutputFormat Hadoop (mapreduce) output format
   * @param job job instance for configuration
   */
  public HadoopValueOutputFormat(OutputFormat<Void, T> mapreduceOutputFormat, Job job) {
    super(mapreduceOutputFormat, job);
  }

  @Override
  public void writeRecord(T record) throws IOException {
    try {
      this.recordWriter.write(null, record);
    } catch (InterruptedException e) {
      throw new IOException("Could not write Record.", e);
    }
  }

  @Override
  public String toString() {
    String jobName = this.getConfiguration().get("mapreduce.job.name");
    if (jobName != null) {
      return String.format("HadoopValueOutputFormat[%s]", jobName);
    }
    return super.toString();
  }
}
