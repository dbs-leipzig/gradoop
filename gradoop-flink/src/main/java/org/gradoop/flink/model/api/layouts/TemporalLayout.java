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
package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;

/**
 * The temporal layout defines the Flink internal (DataSet) representation of a
 * {@link org.gradoop.flink.model.api.tpgm.TemporalGraph} and a
 * {@link org.gradoop.flink.model.api.tpgm.TemporalGraphCollection}.
 */
public interface TemporalLayout {

  /**
   * Returns all vertices.
   *
   * @return vertices
   */
  DataSet<TemporalVertex> getVertices();

  /**
   * Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  DataSet<TemporalVertex> getVerticesByLabel(String label);

  /**
   * Returns all edges.
   *
   * @return edges
   */
  DataSet<TemporalEdge> getEdges();

  /**
   * Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  DataSet<TemporalEdge> getEdgesByLabel(String label);

  /**
   * Returns a dataset containing a single graph head associated with that temporal graph.
   *
   * @return 1-element dataset
   */
  DataSet<TemporalGraphHead> getGraphHead();

  /**
   * Returns the graph heads associated with the logical graphs in that collection.
   *
   * @return temporal graph heads
   */
  DataSet<TemporalGraphHead> getGraphHeads();

  /**
   * Returns graph heads associated with the temporal graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return temporal graph heads
   */
  DataSet<TemporalGraphHead> getGraphHeadsByLabel(String label);
}
