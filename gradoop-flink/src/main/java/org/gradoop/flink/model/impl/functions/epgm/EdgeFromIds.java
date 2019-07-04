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
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

/**
 * Initializes an {@link EPGMEdge} from a given {@link GradoopId} triple.
 * The triple contains (in that order) {@code edge id}, {@code source vertex id} and
 * {@code target vertex id}
 * <p>
 * Forwarded fields:
 * <p>
 * {@code f0->id:        edge id}
 * <p>
 * {@code f1->sourceId:  source vertex id}
 * <p>
 * {@code f2->targetId:  target vertex id}
 */
@FunctionAnnotation.ForwardedFields("f0->id;f1->sourceId;f2->targetId")
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
