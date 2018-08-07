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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
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
