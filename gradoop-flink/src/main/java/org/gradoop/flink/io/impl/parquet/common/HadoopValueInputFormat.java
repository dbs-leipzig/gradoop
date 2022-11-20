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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormatBase;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * InputFormat implementation allowing to use Hadoop (mapreduce) InputFormats which don't provide a key with
 * Flink.
 *
 * @param <T> Value Type
 */
public class HadoopValueInputFormat<T> extends HadoopInputFormatBase<Void, T, T> implements
  ResultTypeQueryable<T> {

  /**
   * Creates a new Flink input format.
   *
   * @param mapreduceInputFormat  Hadoop (mapreduce) input format
   * @param value value type class
   * @param job job instance for configuration
   */
  public HadoopValueInputFormat(InputFormat<Void, T> mapreduceInputFormat, Class<T> value, Job job) {
    super(mapreduceInputFormat, Void.class, value, job);
  }

  @Override
  public T nextRecord(T record) throws IOException {
    if (!this.fetched) {
      fetchNext();
    }
    if (!this.hasNext) {
      return null;
    }
    try {
      record = recordReader.getCurrentValue();
    } catch (InterruptedException e) {
      throw new IOException("Could not get KeyValue pair.", e);
    }
    this.fetched = false;

    return record;
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return TypeInformation.of(this.valueClass);
  }

  @Override
  public String toString() {
    String jobName = this.getConfiguration().get("mapreduce.job.name");
    if (jobName != null) {
      return String.format("HadoopValueInputFormat[%s]", jobName);
    }
    return super.toString();
  }
}
