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

package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.flink.io.impl.rdbms.tuples.LabelIdKeyTuple;

/**
 * Creates edges from joined primary key tables respectively foreign key tables
 * of foreign key relations
 */
public class JoinSetToEdges
    extends RichMapFunction<Tuple2<LabelIdKeyTuple, LabelIdKeyTuple>, Edge> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 1L;

  /**
   * Gradoop edge factory
   */
  private EdgeFactory edgeFactory;

  /**
   * Valid gradoop edge factory
   *
   * @param edgeFactory
   *             Valid gradoop edge factory
   */
  public JoinSetToEdges(EdgeFactory edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public Edge map(Tuple2<LabelIdKeyTuple, LabelIdKeyTuple> preEdge) throws Exception {
    GradoopId id = GradoopId.get();
    GradoopId sourceVertexId = preEdge.f1.f1;
    GradoopId targetVertexId = preEdge.f0.f1;
    String label = preEdge.f0.f0;
    return edgeFactory.initEdge(id, label, sourceVertexId, targetVertexId);
  }
}
