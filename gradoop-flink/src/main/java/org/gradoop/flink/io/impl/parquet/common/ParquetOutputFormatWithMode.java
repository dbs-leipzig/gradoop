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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;

import java.io.IOException;

/**
 * Extension of the {@link ParquetOutputFormat<T>} that allows specifying a file creation mode.
 *
 * @param <T> the type of the materialized records
 */
public class ParquetOutputFormatWithMode<T> extends ParquetOutputFormat<T> {

  /**
   * File creation mode configuration key
   */
  private static final String PARQUET_WRITER_MODE = "parquet.writer.mode";

  /**
   * Sets the file creation mode of a {@link JobContext}.
   *
   * @param context the job's context
   * @param mode the file creation mode
   */
  public static void setFileCreationMode(JobContext context, ParquetFileWriter.Mode mode) {
    ContextUtil.getConfiguration(context).setEnum(PARQUET_WRITER_MODE, mode);
  }

  /**
   * Gets the file creation mode of a {@link JobContext}.
   *
   * @param context the job's context
   * @return the specified mode or defaults to {@link ParquetFileWriter.Mode#CREATE} if not found
   */
  public static ParquetFileWriter.Mode getFileCreationMode(JobContext context) {
    return ContextUtil.getConfiguration(context).getEnum(PARQUET_WRITER_MODE, ParquetFileWriter.Mode.CREATE);
  }

  @Override
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    return super.getRecordWriter(taskAttemptContext, getFileCreationMode(taskAttemptContext));
  }
}
