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
package org.gradoop.flink.io.impl.parquet.plain;

import org.apache.flink.api.java.DataSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.gradoop.flink.io.impl.parquet.common.HadoopValueInputFormat;
import org.gradoop.flink.io.impl.parquet.common.HadoopValueOutputFormat;
import org.gradoop.flink.io.impl.parquet.common.ParquetOutputFormatWithMode;
import org.gradoop.flink.io.impl.parquet.plain.common.GradoopReadSupport;
import org.gradoop.flink.io.impl.parquet.plain.common.GradoopRootConverter;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * Base class for parquet data source and data sink.
 */
abstract class ParquetBase {

  /**
   * File ending for parquet-protobuf files.
   */
  private static final String PARQUET_FILE_SUFFIX = ".parquet";
  /**
   * parquet-protobuf file for vertices.
   */
  private static final String VERTEX_FILE = "vertices" + PARQUET_FILE_SUFFIX;
  /**
   * parquet-protobuf file for graph heads.
   */
  private static final String GRAPH_HEAD_FILE = "graphs" + PARQUET_FILE_SUFFIX;
  /**
   * parquet-protobuf file for edges.
   */
  private static final String EDGE_FILE = "edges" + PARQUET_FILE_SUFFIX;

  /**
   * Root directory containing the CSV and metadata files.
   */
  private final String basePath;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Constructor.
   *
   * @param basePath directory to the parquet files
   * @param config   Gradoop Flink configuration
   */
  protected ParquetBase(String basePath, GradoopFlinkConfig config) {
    Objects.requireNonNull(basePath);
    Objects.requireNonNull(config);

    this.basePath = basePath.endsWith(File.separator) ? basePath : basePath + File.separator;
    this.config = config;
  }

  /**
   * Returns the path to the graph head file.
   *
   * @return graph head file path
   */
  protected String getGraphHeadPath() {
    return basePath + GRAPH_HEAD_FILE;
  }

  /**
   * Returns the path to the vertex file.
   *
   * @return vertex file path
   */
  protected String getVertexPath() {
    return basePath + VERTEX_FILE;
  }

  /**
   * Returns the path to the edge file.
   *
   * @return edge file path
   */
  protected String getEdgePath() {
    return basePath + EDGE_FILE;
  }

  /**
   * Get the gradoop configuration.
   *
   * @return the gradoop configuration instance
   */
  protected GradoopFlinkConfig getConfig() {
    return config;
  }

  /**
   * Writes a {@link DataSet} as parquet file(s) to the specified location.
   *
   * @param data the {@link DataSet}
   * @param outputPath the path of the file
   * @param writeSupport the write support for the datasets type
   * @param overwrite the behavior regarding existing files
   * @param <T> the datasets object type
   * @throws IOException if an I/O error occurs
   */
  protected <T> void write(DataSet<T> data, String outputPath,
    Class<? extends WriteSupport<T>> writeSupport, boolean overwrite) throws IOException {
    Job job = Job.getInstance();

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ParquetOutputFormat.setEnableDictionary(job, false);
    ParquetOutputFormat.setWriteSupportClass(job, writeSupport);
    ParquetOutputFormatWithMode.setFileCreationMode(job,
      overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE);

    data.output(new HadoopValueOutputFormat<T>(new ParquetOutputFormatWithMode<>(), job));
  }

  /**
   * Creates a {@link DataSet} that represents the elements produced by reading the given parquet file.
   *
   * @param type the element type
   * @param inputPath the path of the file
   * @param rootConverter the root converter for the element type
   * @return A {@link DataSet} that represents the data read from the given file.
   * @param <T> element type
   * @throws IOException if an I/O error occurs
   */
  protected <T> DataSet<T> read(Class<T> type, String inputPath,
    Class<? extends GradoopRootConverter<T>> rootConverter) throws IOException {
    Job job = Job.getInstance();

    FileInputFormat.addInputPath(job, new Path(inputPath));
    GradoopReadSupport.setRootConverter(job, rootConverter);
    ParquetInputFormat.setReadSupportClass(job, GradoopReadSupport.class);

    return config.getExecutionEnvironment().createInput(
      new HadoopValueInputFormat<T>(new ParquetInputFormat<>(GradoopReadSupport.class), type, job));
  }
}
