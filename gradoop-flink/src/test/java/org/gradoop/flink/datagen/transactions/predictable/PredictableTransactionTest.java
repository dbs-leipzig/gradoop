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
package org.gradoop.flink.datagen.transactions.predictable;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PredictableTransactionTest extends GradoopFlinkTestBase {

  @Test
  public void testMaxVertexLabel() throws Exception {
    for(long maxVertexLabel = 0; maxVertexLabel < 10; maxVertexLabel++) {
      // multigraph
      GraphTransaction graph = new PredictableTransaction(
        1, true, getConfig()).map(maxVertexLabel);

      assertEquals(
        (maxVertexLabel % 10 + 1) * 9 + 1, graph.getVertices().size());
      assertEquals(
        (maxVertexLabel % 10 + 1) * 14, graph.getEdges().size());

      // simple graph
      graph = new PredictableTransaction(
        1, false, getConfig()).map(maxVertexLabel);

      assertEquals(
        (maxVertexLabel % 10 + 1) * 8 + 1, graph.getVertices().size());
      assertEquals(
        (maxVertexLabel % 10 + 1) * 10, graph.getEdges().size());
    }
  }

  @Test
  public void testGraphSize() throws Exception {
    // multigraph
    GraphTransaction size1 = new PredictableTransaction(
      1, true, getConfig()).map(7L);

    GraphTransaction size2 = new PredictableTransaction(
      2, true, getConfig()).map(7L);

    assertEquals(
      (size1.getVertices().size() - 1),
      (size2.getVertices().size() - 1) / 2 );

    // simple graph
    size1 = new PredictableTransaction(1, false, getConfig()).map(7L);
    size2 = new PredictableTransaction(2, false, getConfig()).map(7L);

    assertEquals(
      (size1.getVertices().size() - 1),
      (size2.getVertices().size() - 1) / 2 );
  }
}