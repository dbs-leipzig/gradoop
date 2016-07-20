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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.io.impl.graph.tuples.ImportEdge;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * MapFunction to create ImportEdges
 */
public class UniqueEdgeIdsToImportEdge
  implements MapFunction<Tuple2<Long, Tuple2<Long, Long>>, ImportEdge<Long>> {

  /**
   * reused ImportEdge
   */
  private ImportEdge<Long> reuseEdge;

  /**
   * Constructor
   */
  public UniqueEdgeIdsToImportEdge() {
    this.reuseEdge = new ImportEdge<>();
  }

  /**
   * Method to create ImportEdge
   *
   * @param idTuple     tuple that contains unique line id + source and
   *                    target ids
   * @return            initialized reuseEdge
   * @throws Exception
   */
  @Override
  public ImportEdge<Long> map(Tuple2<Long, Tuple2<Long, Long>> idTuple)
      throws Exception {
    reuseEdge.setId(idTuple.f0);
    reuseEdge.setSourceVertexId(idTuple.f1.f0);
    reuseEdge.setTargetVertexId(idTuple.f1.f1);
    reuseEdge.setLabel("");
    reuseEdge.setProperties(PropertyList.create());
    return reuseEdge;
  }
}
