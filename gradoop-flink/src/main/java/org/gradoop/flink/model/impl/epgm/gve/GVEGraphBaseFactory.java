/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.epgm.gve;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Objects;

/**
 * Base class for constructing GVE-based graphs and graph collections.
 */
abstract class GVEGraphBaseFactory {

  /**
   * Configuration
   */
  GradoopFlinkConfig config;

  /**
   * Creates a new GVE-based factory.
   *
   * @param config Gradoop config
   */
  GVEGraphBaseFactory(GradoopFlinkConfig config) {
    Objects.requireNonNull(config);
    this.config = config;
  }

  /**
   * Creates a graph head dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param graphHeads  graph heads
   * @return graph head dataset
   */
  DataSet<GraphHead> createGraphHeadDataSet(Collection<GraphHead> graphHeads) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<GraphHead> graphHeadSet;
    if (graphHeads.isEmpty()) {
      graphHeadSet = config.getExecutionEnvironment()
        .fromElements(config.getGraphHeadFactory().createGraphHead())
        .filter(new False<GraphHead>());
    } else {
      graphHeadSet =  env.fromCollection(graphHeads);
    }
    return graphHeadSet;
  }

  /**
   * Creates a vertex dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param vertices  vertex collection
   * @return vertex dataset
   */
  DataSet<Vertex> createVertexDataSet(Collection<Vertex> vertices) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<Vertex> vertexSet;
    if (vertices.isEmpty()) {
      vertexSet = config.getExecutionEnvironment()
        .fromElements(config.getVertexFactory().createVertex())
        .filter(new False<Vertex>());
    } else {
      vertexSet = env.fromCollection(vertices);
    }
    return vertexSet;
  }

  /**
   * Creates an edge dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param edges edge collection
   * @return edge dataset
   */
  DataSet<Edge> createEdgeDataSet(Collection<Edge> edges) {

    DataSet<Edge> edgeSet;
    if (edges.isEmpty()) {
      GradoopId dummyId = GradoopId.get();
      edgeSet = config.getExecutionEnvironment()
        .fromElements(config.getEdgeFactory().createEdge(dummyId, dummyId))
        .filter(new False<Edge>());
    } else {
      edgeSet = config.getExecutionEnvironment().fromCollection(edges);
    }
    return edgeSet;
  }
}
