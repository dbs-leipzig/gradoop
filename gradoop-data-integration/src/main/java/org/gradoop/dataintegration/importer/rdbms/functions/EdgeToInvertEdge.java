/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.rdbms.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;

/**
 * Creates edges with opposite direction
 */
public class EdgeToInvertEdge implements MapFunction<Edge, Edge> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Gradoop edge factory
   */
  private EdgeFactory edgeFactory;

  /**
   * Creates other direction edge
   *
   * @param edgeFactory Valid gradoop edge factory
   */
  EdgeToInvertEdge(EdgeFactory edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public Edge map(Edge e) {
    GradoopId sourceId = e.getSourceId();
    e.setSourceId(e.getTargetId());
    e.setTargetId(sourceId);
    return e;
  }
}
