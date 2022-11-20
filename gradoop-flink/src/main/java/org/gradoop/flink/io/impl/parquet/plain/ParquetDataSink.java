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

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.parquet.plain.write.EdgeWriteSupport;
import org.gradoop.flink.io.impl.parquet.plain.write.GraphHeadWriteSupport;
import org.gradoop.flink.io.impl.parquet.plain.write.VertexWriteSupport;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * A graph data sink for parquet files.
 */
public class ParquetDataSink extends ParquetBase implements DataSink {

  /**
   * Creates a new parquet data sink.
   *
   * @param basePath directory to the parquet-protobuf files
   * @param config   Gradoop Flink configuration
   */
  public ParquetDataSink(String basePath, GradoopFlinkConfig config) {
    super(basePath, config);
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(logicalGraph.getCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    write(graphCollection.getGraphHeads(), getGraphHeadPath(), GraphHeadWriteSupport.class, overwrite);
    write(graphCollection.getVertices(), getVertexPath(), VertexWriteSupport.class, overwrite);
    write(graphCollection.getEdges(), getEdgePath(), EdgeWriteSupport.class, overwrite);
  }
}
