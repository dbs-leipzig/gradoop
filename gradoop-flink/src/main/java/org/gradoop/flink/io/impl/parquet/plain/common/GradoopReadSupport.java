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
package org.gradoop.flink.io.impl.parquet.plain.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Simple parquet read support for a {@link GradoopRootConverter<R>}
 *
 * @param <R> the record type
 */
public class GradoopReadSupport<R> extends ReadSupport<R> {

  /**
   * The configuration key for the {@link GradoopRootConverter}
   */
  public static final String GRADOOP_ROOT_CONVERTER_CLASS = "gradoop.parquet.rootConverterClass";

  /**
   * The constructor of the root converter
   */
  private Constructor<? extends GradoopRootConverter> rootConverterConstructor;

  /**
   * Sets the root converter class of a {@link JobContext}.
   *
   * @param job the job's context
   * @param rootConverter the root converter class
   */
  public static void setRootConverter(Job job, Class<? extends GradoopRootConverter<?>> rootConverter) {
    ContextUtil.getConfiguration(job)
      .setClass(GRADOOP_ROOT_CONVERTER_CLASS, rootConverter, GradoopRootConverter.class);
  }

  /**
   * Gets the root converter class of a job's {@link Configuration}.
   *
   * @param configuration the job's configuration
   * @return the specified root converter class or defaults to null
   */
  public static Class<? extends GradoopRootConverter> getRootConverter(Configuration configuration) {
    return configuration.getClass(GRADOOP_ROOT_CONVERTER_CLASS, null, GradoopRootConverter.class);
  }

  @Override
  public ReadContext init(InitContext context) {
    try {
      Class<? extends GradoopRootConverter> rootConverter = getRootConverter(context.getConfiguration());
      this.rootConverterConstructor = rootConverter.getConstructor(MessageType.class);
    } catch (ReflectiveOperationException | SecurityException e) {
      throw new RuntimeException("can't initialize ReadSupport", e);
    }

    return new ReadContext(context.getFileSchema());
  }

  @Override
  public RecordMaterializer<R> prepareForRead(Configuration configuration,
    Map<String, String> keyValueMetaData,
    MessageType fileSchema, ReadContext readContext) {
    try {
      GradoopRootConverter rootConverter = this.rootConverterConstructor
        .newInstance(readContext.getRequestedSchema());
      return new GradoopRecordMaterializer<>(rootConverter);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("can't initialize GradoopRecordMaterializer", e);
    }
  }
}
