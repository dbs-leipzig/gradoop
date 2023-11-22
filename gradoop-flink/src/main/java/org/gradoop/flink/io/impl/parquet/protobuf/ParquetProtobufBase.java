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
package org.gradoop.flink.io.impl.parquet.protobuf;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.flink.api.java.DataSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.proto.ProtoParquetInputFormat;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.gradoop.flink.io.impl.parquet.common.HadoopValueInputFormat;
import org.gradoop.flink.io.impl.parquet.common.HadoopValueOutputFormat;
import org.gradoop.flink.io.impl.parquet.common.ParquetOutputFormatWithMode;
import org.gradoop.flink.io.impl.parquet.protobuf.kryo.ProtobufBuilderKryoSerializer;
import org.gradoop.flink.io.impl.parquet.protobuf.kryo.ProtobufMessageKryoSerializer;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * Base class for parquet-protobuf data source and data sink.
 */
abstract class ParquetProtobufBase {

  /**
   * File ending for parquet-protobuf files.
   */
  private static final String PROTOBUF_PARQUET_FILE_SUFFIX = ".proto.parquet";
  /**
   * parquet-protobuf file for vertices.
   */
  private static final String VERTEX_FILE = "vertices" + PROTOBUF_PARQUET_FILE_SUFFIX;
  /**
   * parquet-protobuf file for graph heads.
   */
  private static final String GRAPH_HEAD_FILE = "graphs" + PROTOBUF_PARQUET_FILE_SUFFIX;
  /**
   * parquet-protobuf file for edges.
   */
  private static final String EDGE_FILE = "edges" + PROTOBUF_PARQUET_FILE_SUFFIX;

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
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  protected ParquetProtobufBase(String basePath, GradoopFlinkConfig config) {
    Objects.requireNonNull(basePath);
    Objects.requireNonNull(config);

    this.basePath = basePath.endsWith(File.separator) ? basePath : basePath + File.separator;
    this.config = config;

    // see: https://github.com/apache/flink/pull/7865
    config.getExecutionEnvironment()
      .addDefaultKryoSerializer(Message.Builder.class, ProtobufBuilderKryoSerializer.class);
    config.getExecutionEnvironment()
      .addDefaultKryoSerializer(Message.class, ProtobufMessageKryoSerializer.class);
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
   * Writes a protobuf {@link DataSet} as parquet-protobuf file(s) to the specified location.
   *
   * @param data the protobuf {@link DataSet}
   * @param type the protobuf object class
   * @param outputPath the path of the file
   * @param overwrite the behavior regarding existing files
   * @param <T> protobuf object type
   * @throws IOException if an I/O error occurs
   */
  public <T extends MessageOrBuilder> void write(DataSet<T> data, Class<? extends Message> type,
    String outputPath, boolean overwrite) throws IOException {
    Job job = Job.getInstance();

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ParquetOutputFormat.setEnableDictionary(job, false);
    ParquetOutputFormat.setWriteSupportClass(job, ProtoWriteSupport.class);
    ParquetOutputFormatWithMode.setFileCreationMode(job,
      overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE);
    ProtoWriteSupport.setSchema(ContextUtil.getConfiguration(job), type);
    ProtoWriteSupport.setWriteSpecsCompliant(ContextUtil.getConfiguration(job), true);

    data.output(new HadoopValueOutputFormat<T>(new ParquetOutputFormatWithMode<>(), job));
  }

  /**
   * Creates a {@link DataSet} that represents the protobuf objects produced by reading the given
   * parquet-protobuf file.
   *
   * @param type the protobuf object class
   * @param inputPath the path of the file
   * @return A {@link DataSet} that represents the data read from the given file.
   * @param <T> protobuf object type
   * @throws IOException if an I/O error occurs
   */
  public <T extends Message.Builder> DataSet<T> read(Class<T> type, String inputPath) throws
    IOException {
    Job job = Job.getInstance();

    FileInputFormat.addInputPath(job, new Path(inputPath));
    ProtoReadSupport.setProtobufClass(ContextUtil.getConfiguration(job),
      type.getDeclaringClass().asSubclass(Message.class).getName());

    return config.getExecutionEnvironment()
      .createInput(new HadoopValueInputFormat<T>(new ProtoParquetInputFormat<>(), type, job));
  }
}
