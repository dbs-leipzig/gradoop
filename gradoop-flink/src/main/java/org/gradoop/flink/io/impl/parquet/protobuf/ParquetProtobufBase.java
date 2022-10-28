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
import org.gradoop.flink.io.impl.parquet.protobuf.kryo.ProtobufBuilderKryoSerializer;
import org.gradoop.flink.io.impl.parquet.protobuf.kryo.ProtobufMessageKryoSerializer;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for Parquet-Protobuf data source and data sink.
 */
public abstract class ParquetProtobufBase {

  /**
   * Constructor.
   *
   * @param basePath directory to the Parquet files
   * @param config Gradoop Flink configuration
   */
  protected ParquetProtobufBase(String basePath, GradoopFlinkConfig config) {
    // see: https://github.com/apache/flink/pull/7865
    config.getExecutionEnvironment()
      .addDefaultKryoSerializer(Message.Builder.class, ProtobufBuilderKryoSerializer.class);
    config.getExecutionEnvironment()
      .addDefaultKryoSerializer(Message.class, ProtobufMessageKryoSerializer.class);
  }
}
