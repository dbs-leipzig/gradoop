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

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;


public class NewTicket<V extends EPGMVertex> extends
  RichGroupReduceFunction<V, V> {

  public static final String TICKET_REASON_LATE_DELIVERY = "lateDelivery";
  public static final String TICKET_REASON_BAD_QUALITY = "badQuality";
  private final EPGMVertexFactory<V> vertexFactory;

  private String ticketReason;

  public NewTicket(EPGMVertexFactory<V> vertexFactory, String ticketReason) {
    this.vertexFactory = vertexFactory;
    this.ticketReason = ticketReason;
  }

  @Override
  public void reduce(Iterable<V> values, Collector<V> out) throws Exception {

  }
}
