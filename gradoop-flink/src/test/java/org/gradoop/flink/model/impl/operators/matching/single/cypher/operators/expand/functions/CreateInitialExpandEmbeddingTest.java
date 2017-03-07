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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples
  .EdgeWithTiePoint;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CreateInitialExpandEmbeddingTest {
  private final GradoopId m = GradoopId.get();
  private final GradoopId n = GradoopId.get();
  private final GradoopId a = GradoopId.get();

  private final GradoopId e0 = GradoopId.get();
  private final GradoopId e1 = GradoopId.get();

  // Homomorphism
  @Test
  public void testWithoutDuplicates() throws Exception {
    testJoin(buildEdge(n, e1, a), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithDuplicateBaseVertex() throws Exception {
    testJoin(buildEdge(n, e1, m), new ArrayList<>(), new ArrayList<>(), -1, true);
    testJoin(buildEdge(n, e1, n), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithDuplicateBaseEdge() throws Exception {
    testJoin(buildEdge(n ,e0 ,a), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithLoop() throws Exception {
    testJoin(buildEdge(n,e1,m), new ArrayList<>(), new ArrayList<>(), 0, true);
  }

  //VertexIsomorphism
  @Test
  public void testVertexIsomorphismWithoutDuplicates() throws Exception {
    testJoin(buildEdge(n, e1, a), Lists.newArrayList(0,2), new ArrayList<>(), -1, true);
  }

  @Test
  public void testVertexIsomorphismWithDuplicateBaseVertex() throws Exception {
    testJoin(buildEdge(n, e1, m), Lists.newArrayList(0,2), new ArrayList<>(), -1, false);
    testJoin(buildEdge(n, e1, n), Lists.newArrayList(0,2), new ArrayList<>(), -1, false);
  }

  @Test
  public void testVertexIsomorphismWithDuplicateBaseEdge() throws Exception {
    testJoin(buildEdge(n, e0, a), Lists.newArrayList(0,2), new ArrayList<>(), -1, true);
  }

  @Test
  public void testVertexIsomorphismWithLoop() throws Exception {
    testJoin(buildEdge(n,e1,m), Lists.newArrayList(0,2), new ArrayList<>(), 0, true);
    testJoin(buildEdge(n,e1,n), Lists.newArrayList(0,2), new ArrayList<>(), 2, true);
  }

  //EdgeIsomorphism
  @Test
  public void testEdgeIsomorphismWithoutDuplicates() throws Exception {
    testJoin(buildEdge(n, e1, a), new ArrayList<>(), Lists.newArrayList(0,1), -1, true);
  }

  @Test
  public void testEdgeIsomorphismWithDuplicateBaseVertex() throws Exception {
    testJoin(buildEdge(n, e1, m), new ArrayList<>(), Lists.newArrayList(0,1), -1, true);
    testJoin(buildEdge(n, e1, n), new ArrayList<>(), Lists.newArrayList(0,1), -1, true);
  }

  @Test
  public void testEdgeIsomorphismWithDuplicateBaseEdge() throws Exception {
    testJoin(buildEdge(n, e0, a), new ArrayList<>(), Lists.newArrayList(0,1), -1, false);
  }

  @Test
  public void testEdgeIsomorphismWithLoop() throws Exception {
    testJoin(buildEdge(n, e1, m), new ArrayList<>(), Lists.newArrayList(0,1), 0, true);
    testJoin(buildEdge(n, e0, m), new ArrayList<>(), Lists.newArrayList(0,1), 0, false);
  }


  /**
   * Tests the join of the edge with an Embedding
   * (m, e0, n) x edge
   *
   * @param edge the edge the join is performed with
   * @param distinctVertices distinct vertex columns of the base embedding
   * @param distinctEdges distinct edge columns of the base embedding
   * @param closingColumn closing column
   * @param isResult if true it is expected that the join yields exactly one result, 0 otherwise
   * @throws Exception
   */
  private void testJoin(Embedding edge, List<Integer> distinctVertices,
    List<Integer> distinctEdges, int closingColumn, boolean isResult) throws Exception {

    Embedding base = new Embedding();
    base.add(m);
    base.add(e0);
    base.add(n);

    EdgeWithTiePoint edgeTuple = new EdgeWithTiePoint(edge);

    CreateExpandEmbedding op = new CreateExpandEmbedding(distinctVertices, distinctEdges, closingColumn);

    List<ExpandEmbedding> results = new ArrayList<>();
    op.join(base, edgeTuple, new ListCollector<>(results));

    assertEquals(isResult ? 1:0, results.size());

    if (isResult) {
      assertEquals(base, results.get(0).getBase());
      assertArrayEquals(new GradoopId[]{edge.getId(1)}, results.get(0).getPath());
      assertEquals(edge.getId(2), results.get(0).getEnd());
    }
  }

  /**
   * Builds an Embedding with the three entries resembling an edge
   * @param src the edges source id
   * @param edgeId the edges id
   * @param tgt the edges target id
   * @return Embedding resembling the edge
   */
  private Embedding buildEdge(GradoopId src, GradoopId edgeId, GradoopId tgt) {
    Embedding edge = new Embedding();
    edge.add(src);
    edge.add(edgeId);
    edge.add(tgt);

    return edge;
  }
}
