/*
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
package org.gradoop.flink.model.impl.layouts.gve.indexed;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.layouts.gve.GVELayout;

import java.util.Map;

/**
 * Like {@link GVELayout}, this layout separated between graph head, vertex and edge layouts. In
 * addition, the datasets are separated by labels and accesses by known labels are much more
 * efficient as they avoid duplicating rows during program execution.
 */
public class IndexedGVELayout extends GVELayout implements LogicalGraphLayout, GraphCollectionLayout {
  /**
   * Mapping from graph label to graph heads with that label.
   */
  private final Map<String, DataSet<GraphHead>> graphHeads;
  /**
   * Mapping from vertex label to vertices with that label.
   */
  private final Map<String, DataSet<Vertex>> vertices;
  /**
   * Mapping from edge label to edges with that label.
   */
  private final Map<String, DataSet<Edge>> edges;

  /**
   * Creates a new Indexed GVE Layout.
   *
   * @param graphHeads mapping from label to graph heads
   * @param vertices mapping from label to vertices
   * @param edges mapping from label to edges
   */
  IndexedGVELayout(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices,
    Map<String, DataSet<Edge>> edges) {
    super(
      graphHeads.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during graph head union")),
      vertices.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during vertex union")),
      edges.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during edge union"))
    );
    this.graphHeads = graphHeads;
    this.vertices = vertices;
    this.edges = edges;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return true;
  }

  @Override
  public DataSet<GraphHead> getGraphHeadsByLabel(String label) {
    return graphHeads.get(label);
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return vertices.get(label);
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return edges.get(label);
  }
}
