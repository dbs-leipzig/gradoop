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
package org.gradoop.flink.model.impl.tpgm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.tpgm.TemporalGraph;
import org.gradoop.flink.model.impl.layouts.gve.temporal.TemporalGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * TODO: descriptions
 */
public class TemporalGraphFactory {
  /**
   * The factory to create a temporal graph layout.
   */
  private TemporalGraphLayoutFactory layoutFactory;
  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  public TemporalGraphFactory(GradoopFlinkConfig config) {
    this.config = Preconditions.checkNotNull(config);
  }

  public void setLayoutFactory(TemporalGraphLayoutFactory layoutFactory) {
    this.layoutFactory = layoutFactory;
  }

  /**
   * Creates a {@link TemporalGraph} from datasets of an EPGM logical graph.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *

   * @param vertices Vertex DataSet
   * @param edges Edge DataSet
   * @param graphHead 1-element GraphHead DataSet
   * @return Temporal graph instance
   */
  public TemporalGraph fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges,
    DataSet<GraphHead> graphHead) {
    return new TPGMTemporalGraph(layoutFactory.fromNonTemporalDataSets(graphHead, vertices, edges),
      config);
  }

}
