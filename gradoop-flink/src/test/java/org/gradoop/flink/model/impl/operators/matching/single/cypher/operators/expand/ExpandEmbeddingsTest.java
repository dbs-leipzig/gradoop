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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.*;
import static org.junit.Assert.assertEquals;

public abstract class ExpandEmbeddingsTest extends PhysicalOperatorTest {
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


    DataSet<Embedding> result = getOperator(
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
    DataSet<Embedding> input = createEmbeddings(getExecutionEnvironment(), 1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c),
      createEmbedding(c,e3,d)
    );

    DataSet<Embedding> result = getOperator(
      input, candidateEdges, 0, 2, 4,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEquals(2, result.count());
    assertEmbeddingExists(result, a,e1,b,e2,c);
    assertEmbeddingExists(result, a,e1,b,e2,c,e3,d);
  }

  @Test
  public void testResultForInExpansion() throws Exception{
    DataSet<Embedding> input = createEmbeddings(getExecutionEnvironment(), 1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(b,e1,a),
      createEmbedding(c,e2,b),
      createEmbedding(d,e3,c)
    );

    DataSet<Embedding> result = getOperator(
      input, candidateEdges, 0, 2, 4,
      ExpandDirection.IN, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEquals(2, result.count());
    assertEmbeddingExists(result, a,e1,b,e2,c);
    assertEmbeddingExists(result, a,e1,b,e2,c,e3,d);
  }

  @Test
  public void testUpperBoundRequirement() throws Exception{
    DataSet<Embedding> input = createEmbeddings(getExecutionEnvironment(), 1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c),
      createEmbedding(c,e3,d)
    );

    DataSet<Embedding> result = getOperator(
      input,candidateEdges, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEveryEmbedding(result, (embedding) -> {
      assertEquals(3, embedding.getIdList(1).size());
    });
  }

  @Test
  public void testLowerBoundRequirement() throws Exception{
    DataSet<Embedding> input = createEmbeddings(getExecutionEnvironment(), 1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b),
      createEmbedding(b,e2,c)
    );

    DataSet<Embedding> result = getOperator(
      input,candidateEdges, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),-1
    ).evaluate();

    assertEveryEmbedding(result, (embedding) -> {
      assertEquals(3, embedding.getIdList(1).size());
    });
  }

  @Test
  public void testLowerBound0() throws Exception{
    DataSet<Embedding> input = createEmbeddings(getExecutionEnvironment(), 1, a);

    DataSet<Embedding> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbedding(a,e1,b)
    );

    DataSet<Embedding> result = getOperator(
      input,candidateEdges, 0, 0, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1
    ).evaluate();

    assertEquals(2, result.count());

    assertEmbeddingExists(result, (embedding) ->
      embedding.getId(0).equals(embedding.getId(2)) &&
      embedding.getIdList(1).size() == 0
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

    ExpandEmbeddings op = getOperator(
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

    ExpandEmbeddings op = getOperator(
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

    ExpandEmbeddings op = getOperator(
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

  protected abstract ExpandEmbeddings getOperator(
    DataSet<Embedding> input, DataSet<Embedding> candidateEdges,
    int expandColumn, int lowerBound, int upperBound, ExpandDirection direction,
    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn);

}
