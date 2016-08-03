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

package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps the the edge dictionary to a given graph transaction. The
 * integer-like labels are replaced by those from the dictionary files where
 * the integer value from the old labels matches the corresponding keys from
 * the dictionary.
 *
 */
public class EdgeLabelDecoder extends
  RichMapFunction<GraphTransaction, GraphTransaction> {
  /**
   * Constant for broadcast set containing the edge dictionary.
   */
  public static final String EDGE_DICTIONARY = "edgeDictionary";
  /**
   * Constant string which is added to those edges or vertices which do not
   * have an entry in the dictionary while others have one.
   */
  private static final String EMPTY_LABEL = "";
  /**
   * Map which contains a edge dictionary.
   */
  private Map<Integer, String> edgeDictionary;

  /**
   * {@inheritDoc}
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    edgeDictionary = getRuntimeContext()
      .<HashMap<Integer, String>>getBroadcastVariable(EDGE_DICTIONARY)
      .get(0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphTransaction map(GraphTransaction graphTransaction)
      throws Exception {
    String label;
    for (Edge edge : graphTransaction.getEdges()) {
      label = edgeDictionary.get(Integer.parseInt(edge.getLabel()));
      if (label != null) {
        edge.setLabel(label);
      } else {
        edge.setLabel(edge.getLabel() + EMPTY_LABEL);
      }
    }
    return graphTransaction;
  }
}
