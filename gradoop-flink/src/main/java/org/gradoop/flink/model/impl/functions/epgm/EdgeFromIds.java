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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an {@link Vertex} from a given {@link GradoopId} triple.
 *
 * (edgeId, sourceId, targetId) -> edge
 *
 * Forwarded fields:
 *
 * f0->id:        edge id
 * f1->sourceId:  source vertex id
 * f2->targetId:  target vertex id
 */
@FunctionAnnotation.ForwardedFields("f0->id;f1->sourceId;f2->targetId")
public class EdgeFromIds implements
  MapFunction<Tuple3<GradoopId, GradoopId, GradoopId>, Edge>,
  ResultTypeQueryable<Edge> {

  /**
   * EPGM edge factory
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Constructor
   *
   * @param epgmEdgeFactory EPGM edge factory
   */
  public EdgeFromIds(EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    this.edgeFactory = epgmEdgeFactory;
  }

  /**
   * Initializes an {@link Edge} from a given {@link GradoopId} triple. The
   * triple consists of edge id, source vertex id and target vertex id.
   *
   * @param idTriple triple containing (in that order) edge id, source vertex
   *                 id, target vertex id
   * @return EPGM edge
   * @throws Exception
   */
  @Override
  public Edge map(Tuple3<GradoopId, GradoopId, GradoopId> idTriple) throws
    Exception {
    return edgeFactory.initEdge(idTriple.f0, idTriple.f1, idTriple.f2);
  }

  @Override
  public TypeInformation<Edge> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
