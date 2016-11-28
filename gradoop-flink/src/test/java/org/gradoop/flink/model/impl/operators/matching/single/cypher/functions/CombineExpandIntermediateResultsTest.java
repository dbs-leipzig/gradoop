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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandIntermediateResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CombineExpandIntermediateResultsTest {
  private final GradoopId m = GradoopId.get();
  private final GradoopId n = GradoopId.get();
  private final GradoopId a = GradoopId.get();
  private final GradoopId b = GradoopId.get();
  
  private final GradoopId e0 = GradoopId.get();
  private final GradoopId e1 = GradoopId.get();
  private final GradoopId e2 = GradoopId.get();
  
  // Homomorphism
  @Test
  public void testWithoutDuplicates() throws Exception {
    testJoin(buildEdge(a, e2, b), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithDuplicateBaseVertex() throws Exception {
    testJoin(buildEdge(a,e2,m), new ArrayList<>(), new ArrayList<>(), -1, true);
    testJoin(buildEdge(a,e2,n), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithDuplicatePathVertex() throws Exception {
    testJoin(buildEdge(a,e2,a), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithDuplicateBaseEdge() throws Exception {
    testJoin(buildEdge(a,e0,b), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithDuplicatePathEdge() throws Exception {
    testJoin(buildEdge(a,e1,b), new ArrayList<>(), new ArrayList<>(), -1, true);
  }

  @Test
  public void testHomomorphismWithLoop() throws Exception {
    testJoin(buildEdge(a,e2,m), new ArrayList<>(), new ArrayList<>(), 0, true);
    testJoin(buildEdge(a,e2,n), new ArrayList<>(), new ArrayList<>(), 2, true);
  }
  
  //VertexIsomorphism
  @Test
  public void testVertexIsomorphismWithoutDuplicates() throws Exception {
    testJoin(buildEdge(a, e2, b), Lists.newArrayList(0,2), new ArrayList<>(), -1, true);
  }

  @Test
  public void testVertexIsomorphismWithDuplicateBaseVertex() throws Exception {
    testJoin(buildEdge(a,e2,m), Lists.newArrayList(0,2), new ArrayList<>(), -1, false);
    testJoin(buildEdge(a,e2,n), Lists.newArrayList(0,2), new ArrayList<>(), -1, false);
  }

  @Test
  public void testVertexIsomorphismWithDuplicatePathVertex() throws Exception {
    testJoin(buildEdge(a,e2,a), Lists.newArrayList(0,2), new ArrayList<>(), -1, false);
  }

  @Test
  public void testVertexIsomorphismWithDuplicateBaseEdge() throws Exception {
    testJoin(buildEdge(a,e0,b), Lists.newArrayList(0,2), new ArrayList<>(), -1, true);
  }

  @Test
  public void testVertexIsomorphismWithDuplicatePathEdge() throws Exception {
    testJoin(buildEdge(a,e1,b), Lists.newArrayList(0,2), new ArrayList<>(), -1, true);
  }

  @Test
  public void testVertexIsomorphismWithLoop() throws Exception {
    testJoin(buildEdge(a,e2,m), Lists.newArrayList(0,2), new ArrayList<>(), 0, true);
    testJoin(buildEdge(a,e2,n), Lists.newArrayList(0,2), new ArrayList<>(), 2, true);
  }


  //EdgeIsomorphism
  @Test
  public void testEdgeIsomorphismWithoutDuplicates() throws Exception {
    testJoin(buildEdge(a,e2, b), new ArrayList<>(), Lists.newArrayList(0,1), -1, true);
  }

  @Test
  public void testEdgeIsomorphismWithDuplicateBaseVertex() throws Exception {
    testJoin(buildEdge(a,e2,m), new ArrayList<>(), Lists.newArrayList(0,1), -1, true);
    testJoin(buildEdge(a,e2,n), new ArrayList<>(), Lists.newArrayList(0,1), -1, true);
  }

  @Test
  public void testEdgeIsomorphismWithDuplicatePathVertex() throws Exception {
    testJoin(buildEdge(a,e2,a), new ArrayList<>(), Lists.newArrayList(0,1), -1, true);
  }

  @Test
  public void testEdgeIsomorphismWithDuplicateBaseEdge() throws Exception {
    testJoin(buildEdge(a,e0,b), new ArrayList<>(), Lists.newArrayList(0,1), -1, false);
  }

  @Test
  public void testEdgeIsomorphismWithDuplicatePathEdge() throws Exception {
    testJoin(buildEdge(a,e1,b), new ArrayList<>(), Lists.newArrayList(0,1), -1, false);
  }

  @Test
  public void testEdgeIsomorphismWithLoop() throws Exception {
    testJoin(buildEdge(a,e2,m), new ArrayList<>(), Lists.newArrayList(0,1), 0, true);
    testJoin(buildEdge(a,e1,m), new ArrayList<>(), Lists.newArrayList(0,1), 0, false);
    testJoin(buildEdge(a,e0,m), new ArrayList<>(), Lists.newArrayList(0,1), 0, false);
  }


  private void testJoin(Embedding edge, List<Integer> distinctVertices, List<Integer> distinctEdges,
    int circle, boolean isResult) throws Exception {

    ExpandIntermediateResult expandIntermediateResult = new ExpandIntermediateResult(
      new Embedding(Lists.newArrayList(
        new IdEntry(m),
        new IdEntry(e0),
        new IdEntry(n)
      )), new GradoopId[]{e1,a}
    );

    CombineExpandIntermediateResults
      op = new CombineExpandIntermediateResults(distinctVertices, distinctEdges, circle);
    
    List<ExpandIntermediateResult> results = new ArrayList<>();
    op.join(expandIntermediateResult, edge, new ListCollector<>(results));
    
    assertEquals(isResult ? 1:0, results.size());
    
    if (isResult) {
      assertArrayEquals(new GradoopId[]{e1,a,edge.getEntry(1).getId()}, results.get(0).getPath());
      assertEquals(edge.getEntry(2).getId(), results.get(0).getEnd());
    }
  }
  
  private Embedding buildEdge(GradoopId src, GradoopId edge, GradoopId tgt) {
    return new Embedding(Lists.newArrayList(
      new IdEntry(src),
      new IdEntry(edge),
      new IdEntry(tgt)
    ));
  }
}
