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
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.gradoop.common.GradoopTestUtils.call;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExpandEmbeddingsTPGMNodeTest {

  final ExpansionCriteria noCriteria = new ExpansionCriteria();

  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
    leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    leftInputMetaData.setTimeColumn("v1", 0);

    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setTimeColumn("v1", 0);
    rightInputMetaData.setTimeColumn("e1", 1);
    rightInputMetaData.setTimeColumn("v2", 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    ExpandEmbeddingsTPGMNode node = new ExpandEmbeddingsTPGMNode(
      leftMockNode, rightMockNode,
      "v1", "e1", "v2",
      0, 10, ExpandDirection.OUT,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM, noCriteria);

    EmbeddingTPGMMetaData outputMetaData = node.getEmbeddingMetaData();

    assertThat(outputMetaData.getEntryCount(), is(3));
    assertThat(outputMetaData.getEntryColumn("v1"), is(0));
    assertThat(outputMetaData.getEntryColumn("e1"), is(1));
    assertThat(outputMetaData.getEntryColumn("v2"), is(2));
    assertThat(outputMetaData.getEntryType("v1"), is(EmbeddingTPGMMetaData.EntryType.VERTEX));
    assertThat(outputMetaData.getEntryType("e1"), is(EmbeddingTPGMMetaData.EntryType.PATH));
    assertThat(outputMetaData.getEntryType("v2"), is(EmbeddingTPGMMetaData.EntryType.VERTEX));
    assertThat(outputMetaData.getDirection("e1"), is(ExpandDirection.OUT));
    assertThat(outputMetaData.getTimeColumn("v1"), is(0));
    assertThat(outputMetaData.getTimeColumn("v2"), is(1));
  }

  @Test
  public void testGetDistinctColumnsIsomorphism() throws Exception {
    EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
    leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    leftInputMetaData.setEntryColumn("e2", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
    leftInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
    leftInputMetaData.setTimeColumn("v1", 0);
    leftInputMetaData.setTimeColumn("e1", 1);
    leftInputMetaData.setTimeColumn("v2", 2);
    leftInputMetaData.setTimeColumn("e2", 3);
    leftInputMetaData.setTimeColumn("v3", 4);


    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setTimeColumn("v1", 0);
    rightInputMetaData.setTimeColumn("e1", 1);
    rightInputMetaData.setTimeColumn("v2", 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    ExpandEmbeddingsTPGMNode node = new ExpandEmbeddingsTPGMNode(
      leftMockNode, rightMockNode,
      "v3", "e4", "v4",
      0, 10, ExpandDirection.OUT,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM, noCriteria);

    assertThat(call(ExpandEmbeddingsTPGMNode.class, node, "getDistinctVertexColumns",
      new Class<?>[] {EmbeddingTPGMMetaData.class}, new Object[] {leftInputMetaData}),
      is(asList(0, 2, 4)));
    assertThat(call(ExpandEmbeddingsTPGMNode.class, node, "getDistinctEdgeColumns",
      new Class<?>[] {EmbeddingTPGMMetaData.class}, new Object[] {leftInputMetaData}),
      is(asList(1, 3)));
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
    leftInputMetaData.setTimeColumn("v1", 0);
    leftInputMetaData.setTimeColumn("e1", 1);
    leftInputMetaData.setTimeColumn("v2", 2);
    leftInputMetaData.setTimeColumn("e2", 3);
    leftInputMetaData.setTimeColumn("v3", 4);

    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setTimeColumn("v1", 0);
    rightInputMetaData.setTimeColumn("e1", 1);
    rightInputMetaData.setTimeColumn("v2", 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    ExpandEmbeddingsTPGMNode node = new ExpandEmbeddingsTPGMNode(
      leftMockNode, rightMockNode,
      "v3", "e4", "v4",
      0, 10, ExpandDirection.OUT,
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM, noCriteria);

    assertThat(call(ExpandEmbeddingsTPGMNode.class, node, "getDistinctVertexColumns",
      new Class<?>[] {EmbeddingTPGMMetaData.class}, new Object[] {leftInputMetaData}),
      is(asList()));
    assertThat(call(ExpandEmbeddingsTPGMNode.class, node, "getDistinctEdgeColumns",
      new Class<?>[] {EmbeddingTPGMMetaData.class}, new Object[] {leftInputMetaData}),
      is(asList()));
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
    Long[] aTime = new Long[] {1L, 2L, 3L, 4L};
    Long[] bTime = new Long[] {11L, 12L, 13L, 14L};
    Long[] cTime = new Long[] {21L, 22L, 23L, 24L};
    Long[] dTime = new Long[] {31L, 32L, 33L, 34L};
    Long[] eTime = new Long[] {41L, 42L, 43L, 44L};
    Long[] fTime = new Long[] {51L, 52L, 53L, 54L};
    Long[] gTime = new Long[] {61L, 62L, 63L, 64L};

    EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
    leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    leftInputMetaData.setTimeColumn("v1", 0);

    EmbeddingTPGM embedding1 = createEmbeddingTPGM(new GradoopId[] {a}, new Long[][] {aTime});
    DataSet<EmbeddingTPGM> leftEmbeddings = env.fromElements(embedding1);


    /* ---------------------------------------------------------
     * |  v1   | e1    | v2    |  v1Time  |  e1Time  |  v2Time  |
     * ----------------------------------------------------------
     * | id(a) | id(b) | id(c) |  aTime   |  bTime   |  cTime   |  -> Embedding 2
     * ----------------------------------------------------------
     * | id(c) | id(d) | id(e) |  cTime   |  dTime   |  eTime   |  -> Embedding 3
     * ----------------------------------------------------------
     * | id(e) | id(f) | id(g) |  eTime   |  fTime   |  gTime   |  -> Embedding 4
     * ----------------------------------------------------------
     */

    EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
    rightInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
    rightInputMetaData.setTimeColumn("v1", 0);
    rightInputMetaData.setTimeColumn("e1", 1);
    rightInputMetaData.setTimeColumn("v2", 2);

    EmbeddingTPGM embedding2 = createEmbeddingTPGM(new GradoopId[] {a, b, c},
      new Long[][] {aTime, bTime, cTime});
    EmbeddingTPGM embedding3 = createEmbeddingTPGM(new GradoopId[] {c, d, e},
      new Long[][] {cTime, dTime, eTime});
    EmbeddingTPGM embedding4 = createEmbeddingTPGM(new GradoopId[] {e, f, g},
      new Long[][] {eTime, fTime, gTime});
    DataSet<EmbeddingTPGM> rightEmbeddings = env
      .fromElements(embedding2, embedding3, embedding4);

    MockPlanNode leftChild = new MockPlanNode(leftEmbeddings, leftInputMetaData);
    MockPlanNode rightChild = new MockPlanNode(rightEmbeddings, rightInputMetaData);

    ExpandEmbeddingsTPGMNode node = new ExpandEmbeddingsTPGMNode(leftChild, rightChild,
      "v1", "e1", "v2",
      3, 3, ExpandDirection.OUT,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM, noCriteria);


    /* ------------------------------------------------------------------------
     * |  v1   | e1                              | v2    |  v1Time  |  v2Time  |
     * -------------------------------------------------------------------------
     * | id(a) | [id(b),id(c),id(d),id(e),id(f)] | id(g) |  aTime   |  gTime   | -> Result
     * -------------------------------------------------------------------------
     */

    List<EmbeddingTPGM> result = node.execute().collect();
    assertThat(result.size(), is(1));
    EmbeddingTPGM embedding = result.get(0);
    assertThat(embedding.getId(0), is(a));
    assertThat(embedding.getIdList(1).size(), is(5));
    assertThat(embedding.getIdList(1).get(0), is(b));
    assertThat(embedding.getIdList(1).get(1), is(c));
    assertThat(embedding.getIdList(1).get(2), is(d));
    assertThat(embedding.getIdList(1).get(3), is(e));
    assertThat(embedding.getIdList(1).get(4), is(f));
    assertThat(embedding.getId(2), is(g));
    assertThat(embedding.getTimes(0), is(aTime));
    assertThat(embedding.getTimes(1), is(gTime));
    assertThat(embedding.getTimeData().length, is(2 * 4 * Long.BYTES));
  }
}
