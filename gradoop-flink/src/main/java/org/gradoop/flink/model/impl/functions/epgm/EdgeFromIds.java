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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an {@link EPGMVertex} from a given {@link GradoopId} triple.
 *
 * (edgeId, sourceId, targetId) -> edge
 *
 * Forwarded fields:
 *
 * f0->f0:        edge id
 * f1->f4:  source vertex id
 * f2->f5:  target vertex id
 */
@FunctionAnnotation.ForwardedFields("f0->f0;f1->f4;f2->f5")
public class EdgeFromIds implements
  MapFunction<Tuple3<GradoopId, GradoopId, GradoopId>, EPGMEdge>,
  ResultTypeQueryable<EPGMEdge> {

  /**
   * EPGM edge factory
   */
  private final EdgeFactory<EPGMEdge> edgeFactory;

  /**
   * Constructor
   *
   * @param epgmEdgeFactory EPGM edge factory
   */
  public EdgeFromIds(EdgeFactory<EPGMEdge> epgmEdgeFactory) {
    this.edgeFactory = epgmEdgeFactory;
  }

  /**
   * Initializes an {@link EPGMEdge} from a given {@link GradoopId} triple. The
   * triple consists of edge id, source vertex id and target vertex id.
   *
   * @param idTriple triple containing (in that order) edge id, source vertex
   *                 id, target vertex id
   * @return EPGM edge
   * @throws Exception on failure
   */
  @Override
  public EPGMEdge map(Tuple3<GradoopId, GradoopId, GradoopId> idTriple) throws
    Exception {
    return edgeFactory.initEdge(idTriple.f0, idTriple.f1, idTriple.f2);
  }

  @Override
  public TypeInformation<EPGMEdge> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
