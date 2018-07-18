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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

public class ExpandEmbeddingsForLoopTest extends ExpandEmbeddingsTest {

  protected ExpandEmbeddings getOperator(
    DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn) {

    return new ExpandEmbeddingsForLoop(input, candidateEdges, expandColumn, lowerBound,
      upperBound, direction, distinctVertexColumns, distinctEdgeColumns, closingColumn);
  }

}
