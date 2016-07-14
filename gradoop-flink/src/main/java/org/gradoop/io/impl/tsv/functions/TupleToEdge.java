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

package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * MapFunction to create EPGMEdges
 *
 * @param <E> EPGM edge type class
 */
public class TupleToEdge<E extends EPGMEdge> implements
  MapFunction<Tuple6<String, GradoopId, String, String, GradoopId, String>, E> {

  /**
   * Edge data factory.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates map function
   *
   * @param epgmEdgeFactory edge data factory
   */
  public TupleToEdge(EPGMEdgeFactory<E> epgmEdgeFactory) {
    this.edgeFactory = epgmEdgeFactory;
  }

  /**
   * Creates edges based on tuple6 input
   *
   * @param lineTuple     read data from tsv input
   * @return              EPGMEdge
   * @throws Exception
   */
  @Override
  public E map
  (Tuple6<String, GradoopId, String, String, GradoopId, String> lineTuple)
      throws Exception {

    GradoopId edgeID = GradoopId.get();
    String edgeLabel = "";
    GradoopId sourceID = lineTuple.f1;
    GradoopId targetID = lineTuple.f4;
    PropertyList properties = PropertyList.create();
    GradoopIdSet graphs = GradoopIdSet.fromExisting(GradoopId.get());

    return edgeFactory.initEdge(edgeID, edgeLabel, sourceID, targetID,
      properties, graphs);
  }
}
