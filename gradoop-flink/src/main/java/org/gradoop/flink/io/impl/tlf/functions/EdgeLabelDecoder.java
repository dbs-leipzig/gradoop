/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.tlf.TLFConstants;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

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
      .<HashMap<Integer, String>>getBroadcastVariable(TLFConstants.EDGE_DICTIONARY)
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
        edge.setLabel(edge.getLabel() + TLFConstants.EMPTY_LABEL);
      }
    }
    return graphTransaction;
  }
}
