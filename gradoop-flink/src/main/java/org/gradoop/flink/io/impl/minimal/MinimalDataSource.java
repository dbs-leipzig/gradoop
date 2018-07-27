/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.minimal;

import java.io.IOException;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A data source to create a logical graph
 * that is not in gradoop format.
 *
 * It is necessary to pre-process the edges and vertices with
 * the MinimalEdgeProvider/MinimalVertecProvider to import the
 * external representation into the EPGM.
 */
public class MinimalDataSource implements DataSource {

  /**
  * Gradoop Flink configuration
  */
  private GradoopFlinkConfig config;

  /**
  * All vertices the graph contains.
  */
  private MinimalVertexProvider vertices;

  /**
  * All edges the graph contains.
  */
  private MinimalEdgeProvider edges;

  /**
  * Constructor
  * @param config Gradoop configuration
  * @param vertices all vertices of the graph
  * @param edges all edges of the graph
  */
  public MinimalDataSource(GradoopFlinkConfig config, MinimalVertexProvider vertices,
        MinimalEdgeProvider edges) {

    this.config = config;
    this.vertices = vertices;
    this.edges = edges;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {

    DataSet<ImportVertex<String>> importVertices = vertices.importVertex();

    DataSet<ImportEdge<String>> importEdges = edges.importEdge();

    return new GraphDataSource<>(importVertices, importEdges, getConfig()).getLogicalGraph();
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return getConfig().getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }

  GradoopFlinkConfig getConfig() {
    return config;
  }
}
