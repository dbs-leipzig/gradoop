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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.gradoop.common.GradoopTestUtils.call;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEmbeddingTPGMExists;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CartesianProductTest {
  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
    leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    leftInputMetaData.setPropertyColumn("v1", "age", 0);
    leftInputMetaData.setPropertyColumn("e1", "since", 1);
    leftInputMetaData.setTimeColumn("v1", 0);
    leftInputMetaData.setTimeColumn("v2", 1);

    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e2", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v4", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setPropertyColumn("v3", "age", 0);
    rightInputMetaData.setPropertyColumn("e2", "since", 1);
    rightInputMetaData.setPropertyColumn("v4", "age", 2);
    rightInputMetaData.setTimeColumn("v3", 0);
    rightInputMetaData.setTimeColumn("e2", 1);
    rightInputMetaData.setTimeColumn("v4", 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    CartesianProductNode node = new CartesianProductNode(leftMockNode, rightMockNode,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    EmbeddingTPGMMetaData outputMetaData = node.getEmbeddingMetaData();

    assertThat(outputMetaData.getEntryCount(), is(6));
    assertThat(outputMetaData.getEntryColumn("v1"), is(0));
    assertThat(outputMetaData.getEntryColumn("e1"), is(1));
    assertThat(outputMetaData.getEntryColumn("v2"), is(2));
    assertThat(outputMetaData.getEntryColumn("v3"), is(3));
    assertThat(outputMetaData.getEntryColumn("e2"), is(4));
    assertThat(outputMetaData.getEntryColumn("v4"), is(5));

    assertThat(outputMetaData.getPropertyCount(), is(5));
    assertThat(outputMetaData.getPropertyColumn("v1", "age"), is(0));
    assertThat(outputMetaData.getPropertyColumn("e1", "since"), is(1));
    assertThat(outputMetaData.getPropertyColumn("v3", "age"), is(2));
    assertThat(outputMetaData.getPropertyColumn("e2", "since"), is(3));
    assertThat(outputMetaData.getPropertyColumn("v4", "age"), is(4));

    assertThat(outputMetaData.getTimeCount(), is(5));
    assertThat(outputMetaData.getTimeColumn("v1"), is(0));
    assertThat(outputMetaData.getTimeColumn("v2"), is(1));
    assertThat(outputMetaData.getTimeColumn("v3"), is(2));
    assertThat(outputMetaData.getTimeColumn("e2"), is(3));
    assertThat(outputMetaData.getTimeColumn("v4"), is(4));
  }

  @Test
  public void testGetDistinctColumnsIsomorphism() throws Exception {
    EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
    leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    leftInputMetaData.setEntryColumn("e2", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
    leftInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);

    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v4", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e3", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v5", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setEntryColumn("e4", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
    rightInputMetaData.setEntryColumn("v6", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);

    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    CartesianProductNode node = new CartesianProductNode(leftChild, rightChild,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    assertThat(call(CartesianProductNode.class, node, "getDistinctVertexColumnsLeft"), is(asList(0, 2, 4)));
    assertThat(call(CartesianProductNode.class, node, "getDistinctVertexColumnsRight"), is(asList(0, 2, 4)));
    assertThat(call(CartesianProductNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList(1, 3)));
    assertThat(call(CartesianProductNode.class, node, "getDistinctEdgeColumnsRight"), is(asList(1, 3)));
  }

  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test
  public void testGetDistinctColumnsHomomorphism() throws Exception {
    EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
    leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    leftInputMetaData.setEntryColumn("e2", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
    leftInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);

    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v4", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e3", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v5", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setEntryColumn("e4", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
    rightInputMetaData.setEntryColumn("v6", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);

    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    CartesianProductNode node = new CartesianProductNode(leftChild, rightChild,
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    assertThat(call(CartesianProductNode.class, node, "getDistinctVertexColumnsLeft"), is(asList()));
    assertThat(call(CartesianProductNode.class, node, "getDistinctVertexColumnsRight"), is(asList()));
    assertThat(call(CartesianProductNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList()));
    assertThat(call(CartesianProductNode.class, node, "getDistinctEdgeColumnsRight"), is(asList()));
  }

  @Test
  public void testExecute() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    GradoopId d = GradoopId.get();
    GradoopId e = GradoopId.get();
    GradoopId f = GradoopId.get();
    GradoopId g = GradoopId.get();
    GradoopId h = GradoopId.get();

    Long[] aTimes = new Long[] {1L, 2L, 3L, 4L};
    Long[] bTimes = new Long[] {5L, 6L, 7L, 8L};
    Long[] cTimes = new Long[] {15L, 16L, 17L, 18L};
    Long[] dTimes = new Long[] {25L, 26L, 27L, 28L};
    Long[] eTimes = new Long[] {35L, 36L, 37L, 38L};
    Long[] fTimes = new Long[] {45L, 46L, 47L, 48L};
    Long[] gTimes = new Long[] {55L, 56L, 57L, 58L};
    Long[] hTimes = new Long[] {65L, 66L, 67L, 68L};


    EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
    leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    leftInputMetaData.setPropertyColumn("v1", "age", 0);
    leftInputMetaData.setTimeColumn("v1", 0);

    EmbeddingTPGM embedding1 = createEmbeddingTPGM(new GradoopId[] {a}, new Long[][] {aTimes});
    embedding1.addPropertyValues(PropertyValue.create(42));
    EmbeddingTPGM embedding2 = createEmbeddingTPGM(new GradoopId[] {b}, new Long[][] {bTimes});
    embedding2.addPropertyValues(PropertyValue.create(21));

    DataSet<EmbeddingTPGM> leftEmbeddingTPGMs = env.fromElements(embedding1, embedding2);


    /* --------------------------------------------------------------
     * |  v2   | e1    | v3    | v3.age |  v2Time |  e1Time | v3Time |
     * ----------------------------------
     * | id(c) | id(d) | id(e) |  42    |  cTime  |  dTime  | eTime  |  -> Embedding 3
     * ----------------------------------
     * | id(f) | id(g) | id(h) |  21    |  fTime  |  gTime  | hTime  | -> Embedding 4
     * ----------------------------------
     */

    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setPropertyColumn("v3", "age", 0);
    rightInputMetaData.setTimeColumn("v2", 0);
    rightInputMetaData.setTimeColumn("e1", 1);
    rightInputMetaData.setTimeColumn("v3", 2);


    EmbeddingTPGM embedding3 = createEmbeddingTPGM(new GradoopId[] {c, d, e},
      new Long[][] {cTimes, dTimes, eTimes});
    embedding3.addPropertyValues(PropertyValue.create(42));

    EmbeddingTPGM embedding4 = createEmbeddingTPGM(new GradoopId[] {f, g, h},
      new Long[][] {fTimes, gTimes, hTimes});
    embedding3.addPropertyValues(PropertyValue.create(21));


    DataSet<EmbeddingTPGM> rightEmbeddingTPGMs = env.fromElements(embedding3, embedding4);

    MockPlanNode leftChild = new MockPlanNode(leftEmbeddingTPGMs, leftInputMetaData);
    MockPlanNode rightChild = new MockPlanNode(rightEmbeddingTPGMs, rightInputMetaData);

    CartesianProductNode node = new CartesianProductNode(leftChild, rightChild,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    DataSet<EmbeddingTPGM> result = node.execute();

    assertThat(result.count(), is(4L));

    assertEmbeddingTPGMExists(result, new GradoopId[] {a, f, g, h},
      new Long[][] {aTimes, fTimes, gTimes, hTimes});
    assertEmbeddingTPGMExists(result, new GradoopId[] {b, f, g, h},
      new Long[][] {bTimes, fTimes, gTimes, hTimes});
    assertEmbeddingTPGMExists(result, new GradoopId[] {a, c, d, e},
      new Long[][] {aTimes, cTimes, dTimes, eTimes});
    assertEmbeddingTPGMExists(result, new GradoopId[] {b, c, d, e},
      new Long[][] {bTimes, cTimes, dTimes, eTimes});
  }
}
