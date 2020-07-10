/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEmbeddingTPGMExists;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEveryEmbeddingTPGM;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ExpandEmbeddingsTPGMTest extends PhysicalTPGMOperatorTest {
  //define some criteria
  // no conditions at all
  final ExpansionCriteria noCriteria = new ExpansionCriteria(null, null, null, null, null,
    null, null, null, null, null);
  // tx_to of vertices must be ascending
  final ExpansionCriteria withCriteria = new ExpansionCriteria(null, x -> x.f1 >= x.f0, null, null,
    null, null, null, null, null, null);
  //define some vertices
  private final GradoopId a = GradoopId.get();
  private final Long[] aTime = new Long[] {10L, 50L, 0L, 50L};
  private final GradoopId b = GradoopId.get();
  private final Long[] bTime = new Long[] {10L, 50L, 0L, 50L};
  private final GradoopId c = GradoopId.get();
  private final Long[] cTime = new Long[] {10L, 50L, 0L, 50L};
  private final GradoopId d = GradoopId.get();
  private final Long[] dTime = new Long[] {10L, 50L, 0L, 50L};
  private final GradoopId m = GradoopId.get();
  private final Long[] mTime = new Long[] {10L, 50L, 0L, 50L};
  private final GradoopId n = GradoopId.get();
  private final Long[] nTime = new Long[] {10L, 50L, 10L, 50L};
  // another vertex with smaller tx_to
  private final GradoopId x = GradoopId.get();
  private final Long[] xTime = new Long[] {10L, 42L, 10L, 50L};
  //define some edges
  private final GradoopId e0 = GradoopId.get();
  private final Long[] e0Time = new Long[] {5L, 45L, 5L, 45L};
  private final GradoopId e1 = GradoopId.get();
  private final Long[] e1Time = new Long[] {5L, 45L, 5L, 45L};
  private final GradoopId e2 = GradoopId.get();
  private final Long[] e2Time = new Long[] {5L, 45L, 5L, 45L};
  private final GradoopId e3 = GradoopId.get();
  private final Long[] e3Time = new Long[] {5L, 45L, 5L, 45L};
  private final GradoopId e4 = GradoopId.get();
  private final Long[] e4Time = new Long[] {5L, 45L, 5L, 45L};

  @Test
  public void testOutputFormat() throws Exception {
    DataSet<EmbeddingTPGM> input = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {m, e0, n}, new Long[][] {mTime, e0Time, nTime})
    );

    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {n, e1, a}, new Long[][] {nTime, e1Time, aTime}),
      createEmbeddingTPGM(new GradoopId[] {a, e2, b}, new Long[][] {aTime, e2Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e3, c}, new Long[][] {bTime, e3Time, cTime})
    );


    DataSet<EmbeddingTPGM> result = getOperator(
      input, candidateEdges, 2, 2, 1, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1, noCriteria
    ).evaluate();

    assertEquals(3, result.count());

    assertEveryEmbeddingTPGM(result, embedding -> {
      assertEquals(5, embedding.size());
      assertEquals(4, embedding.getTimeData().length / (4 * Long.BYTES));
      embedding.getIdList(3);
    });

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 1 &&
        Arrays.equals(embedding.getTimes(3), aTime)
    );

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 3 &&
        Arrays.equals(embedding.getTimes(3), bTime)
    );

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 5 &&
        Arrays.equals(embedding.getTimes(3), cTime)
    );

    // now with one invalid path to a vertex with smaller tx_to than its predecessor
    candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {n, e1, a}, new Long[][] {nTime, e1Time, aTime}),
      createEmbeddingTPGM(new GradoopId[] {a, e2, b}, new Long[][] {aTime, e2Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e3, c}, new Long[][] {bTime, e3Time, cTime}),
      createEmbeddingTPGM(new GradoopId[] {a, e1, x}, new Long[][] {aTime, e4Time, xTime})
    );


    result = getOperator(
      input, candidateEdges, 2, 2, 1, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1, withCriteria
    ).evaluate();

    // should yield the same results as above, because c-e1-x must not be extended
    assertEquals(3, result.count());

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 1 &&
        Arrays.equals(embedding.getTimes(3), aTime)
    );

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 3 &&
        Arrays.equals(embedding.getTimes(3), bTime)
    );

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(3)).size() == 5 &&
        Arrays.equals(embedding.getTimes(3), cTime)
    );
  }

  @Test
  public void testResultForOutExpansion() throws Exception {
    DataSet<EmbeddingTPGM> input = createEmbeddings(getExecutionEnvironment(),
      new GradoopId[] {a}, new Long[][] {aTime});

    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e2, c}, new Long[][] {bTime, e2Time, cTime}),
      createEmbeddingTPGM(new GradoopId[] {c, e3, d}, new Long[][] {cTime, e3Time, dTime})
    );

    DataSet<EmbeddingTPGM> result = getOperator(
      input, candidateEdges, 0, 0, 2, 4,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1, withCriteria
    ).evaluate();

    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, a, e1, b, e2, c);
    assertEmbeddingTPGMExists(result, a, e1, b, e2, c, e3, d);

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(1)).size() == 3 &&
        Arrays.equals(embedding.getTimes(1), cTime)
    );

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(1)).size() == 5 &&
        Arrays.equals(embedding.getTimes(1), dTime)
    );

    // now with invalid edge b->x
    candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e3, x}, new Long[][] {bTime, e3Time, xTime})
    );

    result = getOperator(
      input, candidateEdges, 0, 0, 1, 4,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1, withCriteria
    ).evaluate();
    assertEquals(1, result.count());
    assertEmbeddingTPGMExists(result, a, e1, b);

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(1)).size() == 1 &&
        Arrays.equals(embedding.getTimes(1), cTime)
    );
  }

  @Test
  public void testResultForInExpansion() throws Exception {
    DataSet<EmbeddingTPGM> input = createEmbeddings(getExecutionEnvironment(),
      new GradoopId[] {a}, new Long[][] {aTime});

    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {b, e1, a}, new Long[][] {bTime, e1Time, aTime}),
      createEmbeddingTPGM(new GradoopId[] {c, e2, b}, new Long[][] {cTime, e2Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {d, e3, c}, new Long[][] {dTime, e3Time, cTime})
    );

    DataSet<EmbeddingTPGM> result = getOperator(
      input, candidateEdges, 0, 0,
      2, 4,
      ExpandDirection.IN, new ArrayList<>(), new ArrayList<>(), -1, withCriteria
    ).evaluate();

    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, a, e1, b, e2, c);
    assertEmbeddingTPGMExists(result, a, e1, b, e2, c, e3, d);

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(1)).size() == 3 &&
        Arrays.equals(embedding.getTimes(1), cTime)
    );

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(1)).size() == 5 &&
        Arrays.equals(embedding.getTimes(1), dTime)
    );
    // now with invalid edge
    candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {b, e1, a}, new Long[][] {bTime, e1Time, aTime}),
      createEmbeddingTPGM(new GradoopId[] {c, e2, b}, new Long[][] {cTime, e2Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {d, e3, c}, new Long[][] {dTime, e3Time, cTime}),
      createEmbeddingTPGM(new GradoopId[] {x, e3, c}, new Long[][] {xTime, e3Time, cTime})
    );

    result = getOperator(
      input, candidateEdges, 0, 0,
      2, 4,
      ExpandDirection.IN, new ArrayList<>(), new ArrayList<>(), -1, withCriteria
    ).evaluate();

    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, a, e1, b, e2, c);
    assertEmbeddingTPGMExists(result, a, e1, b, e2, c, e3, d);

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(1)).size() == 3 &&
        Arrays.equals(embedding.getTimes(1), cTime)
    );

    assertEmbeddingTPGMExists(
      result,
      embedding -> (embedding.getIdList(1)).size() == 5 &&
        Arrays.equals(embedding.getTimes(1), dTime)
    );
  }

  @Test
  public void testUpperBoundRequirement() throws Exception {
    DataSet<EmbeddingTPGM> input = createEmbeddings(getExecutionEnvironment(),
      new GradoopId[] {a}, new Long[][] {aTime});

    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e2, c}, new Long[][] {bTime, e2Time, cTime}),
      createEmbeddingTPGM(new GradoopId[] {c, e3, d}, new Long[][] {cTime, e3Time, dTime})
    );

    DataSet<EmbeddingTPGM> result = getOperator(
      input, candidateEdges, 0, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1,
      withCriteria
    ).evaluate();

    assertEveryEmbeddingTPGM(result, (embedding) -> {
      assertEquals(3, embedding.getIdList(1).size());
    });

    // with invalid edge
    candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e2, x}, new Long[][] {bTime, e2Time, xTime})
    );

    result = getOperator(
      input, candidateEdges, 0, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1,
      withCriteria
    ).evaluate();
    assertEquals(result.count(), 0);
  }

  @Test
  public void testLowerBoundRequirement() throws Exception {
    DataSet<EmbeddingTPGM> input = createEmbeddings(getExecutionEnvironment(),
      new GradoopId[] {a}, new Long[][] {aTime});

    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e2, c}, new Long[][] {bTime, e2Time, cTime})
    );

    DataSet<EmbeddingTPGM> result = getOperator(
      input, candidateEdges, 0, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),
      -1, withCriteria
    ).evaluate();

    assertEveryEmbeddingTPGM(result, (embedding) -> assertEquals(3, embedding.getIdList(1).size()));

    // with invalid edge
    candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e2, x}, new Long[][] {bTime, e2Time, xTime})
    );

    result = getOperator(
      input, candidateEdges, 0, 0, 2, 2,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(),
      -1, withCriteria
    ).evaluate();
    assertEquals(result.count(), 0);
  }

  @Test
  public void testLowerBound0() throws Exception {
    DataSet<EmbeddingTPGM> input = createEmbeddings(getExecutionEnvironment(),
      new GradoopId[] {a}, new Long[][] {aTime});

    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime})
    );

    DataSet<EmbeddingTPGM> result = getOperator(
      input, candidateEdges, 0, 0, 0, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1, withCriteria
    ).evaluate();

    assertEquals(2, result.count());

    assertEmbeddingTPGMExists(result, (embedding) ->
      embedding.getId(0).equals(embedding.getId(2)) &&
        embedding.getIdList(1).size() == 0 && Arrays.equals(embedding.getTimes(1), aTime)
    );

    //invalid edge
    candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, x}, new Long[][] {aTime, e1Time, xTime})
    );

    result = getOperator(
      input, candidateEdges, 0, 0, 0, 3,
      ExpandDirection.OUT, new ArrayList<>(), new ArrayList<>(), -1, withCriteria
    ).evaluate();

    assertEquals(1, result.count());
    assertArrayEquals(result.collect().get(0).getTimes(1), aTime);
  }

  @Test
  public void testFilterDistinctVertices() throws Exception {
    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e1, b}, new Long[][] {aTime, e1Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e2, a}, new Long[][] {bTime, e2Time, aTime})
    );

    DataSet<EmbeddingTPGM> input = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e0, b}, new Long[][] {aTime, e0Time, bTime})
    );

    ExpandEmbeddingsTPGM op = getOperator(
      input, candidateEdges,
      2, 2, 2, 3,
      ExpandDirection.OUT,
      Lists.newArrayList(0, 2),
      new ArrayList<>(), -1, noCriteria
    );

    DataSet<EmbeddingTPGM> result = op.evaluate();

    assertEquals(0, result.count());
  }

  @Test
  public void testFilterDistinctEdges() throws Exception {
    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e0, b}, new Long[][] {aTime, e0Time, bTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e1, a}, new Long[][] {bTime, e1Time, aTime})
    );

    DataSet<EmbeddingTPGM> input = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e0, b}, new Long[][] {aTime, e0Time, bTime})
    );

    ExpandEmbeddingsTPGM op = getOperator(
      input, candidateEdges,
      2, 2, 1, 2,
      ExpandDirection.OUT,
      new ArrayList<>(),
      Lists.newArrayList(1, 2), -1, withCriteria
    );

    DataSet<EmbeddingTPGM> result = op.evaluate();

    assertEquals(1, result.count());
    assertEmbeddingTPGMExists(result, a, e0, b, e1, a);
  }

  @Test
  public void testCircleCondition() throws Exception {
    DataSet<EmbeddingTPGM> candidateEdges = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {b, e1, c}, new Long[][] {bTime, e1Time, cTime}),
      createEmbeddingTPGM(new GradoopId[] {b, e2, a}, new Long[][] {bTime, e2Time, aTime})
    );

    DataSet<EmbeddingTPGM> input = getExecutionEnvironment().fromElements(
      createEmbeddingTPGM(new GradoopId[] {a, e0, b}, new Long[][] {aTime, e0Time, bTime})
    );

    ExpandEmbeddingsTPGM op = getOperator(
      input, candidateEdges,
      2, 2, 1, 2,
      ExpandDirection.OUT,
      new ArrayList<>(),
      new ArrayList<>(),
      0, withCriteria
    );

    DataSet<EmbeddingTPGM> result = op.evaluate();

    assertEquals(1, result.count());
    assertEmbeddingTPGMExists(result, a, e0, b, e2, a);

    //now with a more strict expansion condition (strictly ascending tx_from)
    ExpansionCriteria cond = new ExpansionCriteria(x -> x.f1 > x.f0, null, null, null, null, null, null,
      null, null, null);

    result = getOperator(
      input, candidateEdges,
      2, 2, 1, 2,
      ExpandDirection.OUT,
      new ArrayList<>(),
      new ArrayList<>(),
      0, cond
    ).evaluate();
    assertEquals(0, result.count());
  }

  protected ExpandEmbeddingsTPGM getOperator(
    DataSet<EmbeddingTPGM> input, DataSet<EmbeddingTPGM> candidateEdges,
    int expandColumn, int startVertexTimeColumn, int lowerBound, int upperBound,
    ExpandDirection direction, List<Integer> distinctVertexColumns,
    List<Integer> distinctEdgeColumns, int closingColumn, ExpansionCriteria criteria) {
    return new ExpandEmbeddingsTPGMBulk(input, candidateEdges, expandColumn, startVertexTimeColumn,
      lowerBound, upperBound, direction, distinctVertexColumns, distinctEdgeColumns,
      closingColumn, criteria);
  }

}
