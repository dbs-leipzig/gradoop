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

package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates an EPGM graph head representing a business transaction graphs.
 * @param <G> graph head type
 */
public class NewBtgGraphHead<G extends EPGMGraphHead>
  implements MapFunction<GradoopId, G>, ResultTypeQueryable<G> {

  /**
   * graph head factory
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Constructor
   * @param graphHeadFactory graph head factory
   */
  public NewBtgGraphHead(EPGMGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public G map(GradoopId graphId) throws Exception {
    G graphHead = graphHeadFactory
      .createGraphHead(BusinessTransactionGraphs.BTG_LABEL);

    graphHead.setId(graphId);

    return graphHead;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<G> getProducedType() {
    return TypeExtractor.createTypeInfo(graphHeadFactory.getType());
  }
}
