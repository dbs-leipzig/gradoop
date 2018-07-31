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
package org.gradoop.flink.model.impl.layouts.gve;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for GVE layout factories.
 */
abstract class GVEBaseFactory extends BaseFactory {

  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads GraphHead DataSet
   * @param vertices Vertex DataSet
   * @param edges Edge DataSet
   * @return GVE layout
   */
  GVELayout create(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    Objects.requireNonNull(graphHeads, "GraphHead DataSet was null");
    Objects.requireNonNull(vertices, "Vertex DataSet was null");
    Objects.requireNonNull(edges, "Edge DataSet was null");
    return new GVELayout(graphHeads, vertices, edges);
  }

  /**
   * Creates a collection layout from the given datasets indexed by label.
   *
   * @param graphHeads Mapping from label to graph head dataset
   * @param vertices Mapping from label to vertex dataset
   * @param edges Mapping from label to edge dataset
   * @return GVE layout
   */
  GVELayout create(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    Objects.requireNonNull(graphHeads);
    Objects.requireNonNull(vertices);
    Objects.requireNonNull(edges);

    return new GVELayout(
      graphHeads.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during graph head union")),
      vertices.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during vertex union")),
      edges.values().stream().reduce(createEdgeDataSet(Collections.EMPTY_LIST), DataSet::union)
    );
  }
}
