/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.api.metadata;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.common.model.impl.metadata.MetaData;

/**
 * Class defining a sink for meta data. Meta data can be written locally or distributed.
 *
 * @param <M> a meta data type
 */
public interface MetaDataSink<M extends MetaData> {
  /**
   * Write the meta data tuples to the specified file. The tuples have the form
   * (element prefix, label, metadata).
   *
   * @param path           path to the meta data file
   * @param metaDataTuples (element prefix (g,v,e), label, meta data) tuples
   * @param writeMode      write mode, overwrite or not
   */
  void writeDistributed(
    String path,
    DataSet<Tuple3<String, String, String>> metaDataTuples,
    FileSystem.WriteMode writeMode);

  /**
   * Write the meta data to a the specified file. The file can be either located in a local file
   * system or in HDFS.
   *
   * @param path       path to the meta data file
   * @param metaData   meta data object
   * @param hdfsConfig hdfs file configuration
   * @param overwrite  write mode, overwrite or not
   */
  void writeLocal(
    String path,
    M metaData,
    Configuration hdfsConfig,
    boolean overwrite);
}
