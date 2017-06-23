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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes a new graph head from a given GradoopId.
 */
public class InitGraphHead implements MapFunction<Tuple1<GradoopId>, GraphHead>,
  ResultTypeQueryable<GraphHead> {
  /**
   * GraphHeadFactory
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Constructor
   *
   * @param epgmGraphHeadFactory graph head factory
   */
  public InitGraphHead(EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory) {
    this.graphHeadFactory = epgmGraphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead map(Tuple1<GradoopId> idTuple) {
    return graphHeadFactory.initGraphHead(idTuple.f0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<GraphHead> getProducedType() {
    return TypeExtractor.createTypeInfo(graphHeadFactory.getType());
  }
}
