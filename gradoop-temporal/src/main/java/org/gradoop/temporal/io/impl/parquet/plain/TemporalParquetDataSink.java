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
package org.gradoop.temporal.io.impl.parquet.plain;

import org.gradoop.flink.io.impl.parquet.plain.ParquetDataSink;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.impl.parquet.plain.write.TemporalEdgeWriteSupport;
import org.gradoop.temporal.io.impl.parquet.plain.write.TemporalGraphHeadWriteSupport;
import org.gradoop.temporal.io.impl.parquet.plain.write.TemporalVertexWriteSupport;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;

/**
 * A temporal graph data sink for parquet files.
 */
public class TemporalParquetDataSink extends ParquetDataSink implements TemporalDataSink {

  /**
   * Creates a new temporal parquet data sink.
   *
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  public TemporalParquetDataSink(String basePath, GradoopFlinkConfig config) {
    super(basePath, config);
  }

  @Override
  public void write(TemporalGraph temporalGraph) throws IOException {
    write(temporalGraph, false);
  }

  @Override
  public void write(TemporalGraphCollection temporalGraphCollection) throws IOException {
    write(temporalGraphCollection, false);
  }

  @Override
  public void write(TemporalGraph temporalGraph, boolean overwrite) throws IOException {
    write(temporalGraph.getCollectionFactory().fromGraph(temporalGraph), overwrite);
  }

  @Override
  public void write(TemporalGraphCollection collection, boolean overwrite) throws IOException {
    write(collection.getGraphHeads(), getGraphHeadPath(), TemporalGraphHeadWriteSupport.class, overwrite);
    write(collection.getVertices(), getVertexPath(), TemporalVertexWriteSupport.class, overwrite);
    write(collection.getEdges(), getEdgePath(), TemporalEdgeWriteSupport.class, overwrite);
  }

  @Override
  protected TemporalGradoopConfig getConfig() {
    return (TemporalGradoopConfig) super.getConfig();
  }
}
