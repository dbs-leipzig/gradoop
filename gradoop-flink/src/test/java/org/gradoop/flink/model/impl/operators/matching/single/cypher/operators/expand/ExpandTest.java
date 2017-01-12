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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ExpandTest extends PhysicalOperatorTest {
  //define some vertices
  private final GradoopId a = GradoopId.get();
  private final GradoopId b = GradoopId.get();
  private final GradoopId c = GradoopId.get();
  private final GradoopId d = GradoopId.get();
  private final GradoopId m = GradoopId.get();
  private final GradoopId n = GradoopId.get();

  //define some edges
  private final GradoopId e0 = GradoopId.get();
  private final GradoopId e1 = GradoopId.get();
  private final GradoopId e2 = GradoopId.get();
  private final GradoopId e3 = GradoopId.get();

  @Test
  public void testOutputFormat() throws Exception {
    DataSet<Embedding> input = getExecutionEnvironment().fromElements(
      createEmbedding(m,e0,n)
    );

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(n,e1,a),
      createEmbedding(a,e2,b),
      createEmbedding(b,e3,c)
    );


    DataSet<Embedding> result = new Expand(
      input, candidateEdges, 2, 1, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEquals(3, result.count());

    assertEveryEmbedding(result, (embedding -> {
      assertEquals(5,embedding.size());
      embedding.getIdList(3);
    }));

    assertEmbeddingExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 1
    );

    assertEmbeddingExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 3
    );

    assertEmbeddingExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 5
    );
  }

  @Test
  public void testResultForOutExpansion() throws Exception {
    DataSet<Embedding> input = createEmbeddings(1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c),
      createEmbedding(c,e3,d)
    );

    DataSet<Embedding> result = new Expand(
      input, candidateEdges, 0, 2, 4,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEquals(2, result.count());
    assertEmbeddingExists(result, a,e1,b,e2,c);
    assertEmbeddingExists(result, a,e1,b,e2,c,e3,d);
  }

  @Test
  public void testResultForInExpansion() throws Exception{
    DataSet<Embedding> input = createEmbeddings(1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(b,e1,a),
      createEmbedding(c,e2,b),
      createEmbedding(d,e3,c)
    );

    DataSet<Embedding> result = new Expand(
      input, candidateEdges, 0, 2, 4,
      ExpandDirection.IN, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEquals(2, result.count());
    assertEmbeddingExists(result, a,e1,b,e2,c);
    assertEmbeddingExists(result, a,e1,b,e2,c,e3,d);
  }

  @Test
  public void testUpperBoundRequirement() throws Exception{
    DataSet<Embedding> input = createEmbeddings(1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c),
      createEmbedding(c,e3,d)
    );

    DataSet<Embedding> result = new Expand(
      input,candidateEdges, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEveryEmbedding(result, (embedding) -> {
      assertEquals(3, embedding.getIdList(1).size());
    });
  }

  @Test
  public void testLowerBoundRequirement() throws Exception{
    DataSet<Embedding> input = createEmbeddings(1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c)
    );

    DataSet<Embedding> result = new Expand(
      input,candidateEdges, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEveryEmbedding(result, (embedding) -> {
      assertEquals(3, embedding.getIdList(1).size());
    });
  }

  @Test
  public void testLowerBound0() throws Exception{
    DataSet<Embedding> input = createEmbeddings(1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b)
    );

    DataSet<Embedding> result = new Expand(
      input,candidateEdges, 0, 0, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1
    ).evaluate();

    assertEquals(2, result.count());

    assertEmbeddingExists(result, (embedding) ->
      embedding.size() == 1 &&  embedding.getId(0).equals(a)
    );
  }

  @Test
  public void testFilterDistinctVertices() throws Exception {
    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,a)
    );

    DataSet<Embedding> input = getExecutionEnvironment().fromElements(
      createEmbedding(a,e0,b)
    );

    Expand op = new Expand(
      input, candidateEdges,
      2, 2, 3,
      ExpandDirection.OUT,
      Lists.newArrayList(0,2),
      new ArrayList<>(), -1
    );

    DataSet<Embedding> result = op.evaluate();

    assertEquals(0, result.count());
  }

  @Test
  public void testFilterDistinctEdges() throws Exception {
    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e0,b),
      createEmbedding(b,e1,a)
    );

    DataSet<Embedding> input = getExecutionEnvironment().fromElements(
      createEmbedding(a,e0,b)
    );

    Expand op = new Expand(
      input, candidateEdges,
      2, 1, 2,
      ExpandDirection.OUT,
      new ArrayList<>(),
      Lists.newArrayList(1,2), -1
    );

    DataSet<Embedding> result = op.evaluate();

    assertEquals(1, result.count());
    assertEmbeddingExists(result, a,e0,b,e1,a);
  }

  @Test
  public void testCircleCondition() throws Exception {
    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(b,e1,c),
      createEmbedding(b,e2,a)
    );

    DataSet<Embedding> input = getExecutionEnvironment().fromElements(
      createEmbedding(a,e0,b)
    );

    Expand op = new Expand(
      input, candidateEdges,
      2, 1, 2,
      ExpandDirection.OUT,
      new ArrayList<>(),
      new ArrayList<>(),
      0
    );

    DataSet<Embedding> result = op.evaluate();

    assertEquals(1, result.count());
    assertEmbeddingExists(result, a,e0,b,e2,a);
  }
}
