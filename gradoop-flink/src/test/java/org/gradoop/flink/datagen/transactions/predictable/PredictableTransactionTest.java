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

package org.gradoop.flink.datagen.transactions.predictable;

import org.gradoop.flink.datagen.transactions.predictable
  .PredictableTransaction;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
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