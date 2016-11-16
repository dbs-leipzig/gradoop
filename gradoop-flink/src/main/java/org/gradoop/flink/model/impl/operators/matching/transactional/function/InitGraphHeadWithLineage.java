/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Initializes a new graph head from a given GradoopId and its lineage information, e.g. the
 * source graph this one was created from.
 */
public class InitGraphHeadWithLineage
  implements MapFunction<Tuple2<GradoopId, GradoopId>, GraphHead>, ResultTypeQueryable<GraphHead> {
  /**
   * GraphHeadFactory
   */
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Constructor
   *
   * @param graphHeadFactory graph head factory
   */
  public InitGraphHeadWithLineage(GraphHeadFactory graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead map(Tuple2<GradoopId, GradoopId> idTuple) {
    GraphHead head = graphHeadFactory.initGraphHead(idTuple.f0);
    Properties properties = Properties.createWithCapacity(1);
    properties.set("lineage", idTuple.f1);
    head.setProperties(properties);
    return head;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<GraphHead> getProducedType() {
    return TypeExtractor.createTypeInfo(graphHeadFactory.getType());
  }
}
