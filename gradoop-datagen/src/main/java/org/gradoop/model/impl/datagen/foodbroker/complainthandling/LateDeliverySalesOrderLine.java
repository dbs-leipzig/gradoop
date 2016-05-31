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

package org.gradoop.model.impl.datagen.foodbroker.complainthandling;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;


public class LateDeliverySalesOrderLine<V extends EPGMVertex> extends
  RichFlatMapFunction<V, V> {

  private final EPGMVertexFactory<V> vertexFactory;

  public LateDeliverySalesOrderLine(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void flatMap(V v, Collector<V> collector) throws Exception {

  }
}
