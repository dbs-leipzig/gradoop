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

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;

/**
 * A simple parquet record materializer for the {@link GradoopRootConverter<R>}.
 *
 * @param <R> the record type
 */
public class GradoopRecordMaterializer<R> extends RecordMaterializer<R> {

  /**
   * the root converter
   */
  private final GradoopRootConverter<R> rootConverter;

  /**
   * Creates a new record materializer for the given root converter.
   *
   * @param rootConverter the root converter
   */
  public GradoopRecordMaterializer(GradoopRootConverter<R> rootConverter) {
    this.rootConverter = rootConverter;
  }

  @Override
  public R getCurrentRecord() {
    return this.rootConverter.getCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
    return this.rootConverter;
  }
}
