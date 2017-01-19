package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.binary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData.EntryType;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.MockPlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.PlanNode;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class JoinEmbeddingsNodeTest extends GradoopFlinkTestBase {

  @Test
  public void testMetaDataInitialization() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    leftInputMetaData.setPropertyColumn("v1", "age", 0);
    leftInputMetaData.setPropertyColumn("e1", "since", 1);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e2", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 2);
    rightInputMetaData.setPropertyColumn("v2", "age", 0);
    rightInputMetaData.setPropertyColumn("e2", "since", 1);
    rightInputMetaData.setPropertyColumn("v3", "age", 2);

    MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
    MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftMockNode, rightMockNode,
      Collections.singletonList("v2"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    EmbeddingMetaData outputMetaData = node.getEmbeddingMetaData();

    assertThat(outputMetaData.getEntryCount(), is(5));
    assertThat(outputMetaData.getEntryColumn("v1"), is(0));
    assertThat(outputMetaData.getEntryColumn("e1"), is(1));
    assertThat(outputMetaData.getEntryColumn("v2"), is(2));
    assertThat(outputMetaData.getEntryColumn("e2"), is(3));
    assertThat(outputMetaData.getEntryColumn("v3"), is(4));

    assertThat(outputMetaData.getPropertyCount(), is(5));
    assertThat(outputMetaData.getPropertyColumn("v1", "age"), is(0));
    assertThat(outputMetaData.getPropertyColumn("e1", "since"), is(1));
    assertThat(outputMetaData.getPropertyColumn("v2", "age"), is(2));
    assertThat(outputMetaData.getPropertyColumn("e2", "since"), is(3));
    assertThat(outputMetaData.getPropertyColumn("v3", "age"), is(4));
  }

  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test
  public void testGetJoinColumns() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e2", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 2);


    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      Collections.singletonList("v2"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    assertThat(getColumns(node, "getJoinColumnsLeft"), is(Arrays.asList(2)));
    assertThat(getColumns(node, "getJoinColumnsRight"), is(Arrays.asList(0)));
  }

  @Test
  public void testGetDistinctColumnsIsomorphism() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    leftInputMetaData.setEntryColumn("e2", EntryType.EDGE, 3);
    leftInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 4);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e3", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v4", EntryType.VERTEX, 2);
    rightInputMetaData.setEntryColumn("e4", EntryType.EDGE, 3);
    rightInputMetaData.setEntryColumn("v5", EntryType.VERTEX, 4);


    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      Collections.singletonList("v3"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    assertThat(getColumns(node, "getDistinctVertexColumnsLeft"), is(Arrays.asList(0, 2)));
    assertThat(getColumns(node, "getDistinctVertexColumnsRight"), is(Arrays.asList(2, 4)));
    assertThat(getColumns(node, "getDistinctEdgeColumnsLeft"), is(Arrays.asList(1, 3)));
    assertThat(getColumns(node, "getDistinctEdgeColumnsRight"), is(Arrays.asList(1, 3)));
  }

  @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
  @Test
  public void testGetDistinctColumnsHomomorphism() throws Exception {
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    leftInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    leftInputMetaData.setEntryColumn("e2", EntryType.EDGE, 3);
    leftInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 4);

    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v3", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e3", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v4", EntryType.VERTEX, 2);
    rightInputMetaData.setEntryColumn("e4", EntryType.EDGE, 3);
    rightInputMetaData.setEntryColumn("v5", EntryType.VERTEX, 4);


    PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
    PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      Collections.singletonList("v3"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    assertThat(getColumns(node, "getDistinctVertexColumnsLeft"), is(Arrays.asList()));
    assertThat(getColumns(node, "getDistinctVertexColumnsRight"), is(Arrays.asList()));
    assertThat(getColumns(node, "getDistinctEdgeColumnsLeft"), is(Arrays.asList()));
    assertThat(getColumns(node, "getDistinctEdgeColumnsRight"), is(Arrays.asList()));
  }

  @SuppressWarnings("unchecked")
  private List<Integer> getColumns(JoinEmbeddingsNode node, String methodName) throws Exception {
    Method m = JoinEmbeddingsNode.class.getDeclaredMethod(methodName);
    m.setAccessible(true);
    return (List<Integer>) m.invoke(node);
  }

  @Test
  public void testExecute() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    GradoopId d = GradoopId.get();
    GradoopId e = GradoopId.get();
    GradoopId f = GradoopId.get();

    /*
     * ------------------
     * |  v1   | v1.age |
     * ------------------
     * | id(a) | 42     | -> Embedding 1
     * ------------------
     * | id(b) | 23     | -> Embedding 2
     * ------------------
     */
    EmbeddingMetaData leftInputMetaData = new EmbeddingMetaData();
    leftInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    leftInputMetaData.setPropertyColumn("v1", "age", 0);

    Embedding embedding1 = new Embedding();
    embedding1.add(a, Collections.singletonList(PropertyValue.create(42)));
    Embedding embedding2 = new Embedding();
    embedding2.add(b, Collections.singletonList(PropertyValue.create(23)));

    DataSet<Embedding> leftEmbeddings = getExecutionEnvironment()
      .fromElements(embedding1, embedding2);

    /*
     * ----------------------------------
     * |  v1   | e1    | v2    | v2.age |
     * ----------------------------------
     * | id(a) | id(c) | id(e) |  84    | -> Embedding 3
     * ----------------------------------
     * | id(b) | id(d) | id(f) |  77    | -> Embedding 4
     * ----------------------------------
     */
    EmbeddingMetaData rightInputMetaData = new EmbeddingMetaData();
    rightInputMetaData.setEntryColumn("v1", EntryType.VERTEX, 0);
    rightInputMetaData.setEntryColumn("e1", EntryType.EDGE, 1);
    rightInputMetaData.setEntryColumn("v2", EntryType.VERTEX, 2);
    rightInputMetaData.setPropertyColumn("v2", "age", 0);

    Embedding embedding3 = new Embedding();
    embedding3.add(a);
    embedding3.add(c);
    embedding3.add(e, Collections.singletonList(PropertyValue.create(84)));
    Embedding embedding4 = new Embedding();
    embedding4.add(b);
    embedding4.add(d);
    embedding4.add(f, Collections.singletonList(PropertyValue.create(77)));

    DataSet<Embedding> rightEmbeddings = getExecutionEnvironment()
      .fromElements(embedding3, embedding4);

    MockPlanNode leftChild = new MockPlanNode(leftEmbeddings, leftInputMetaData);
    MockPlanNode rightChild = new MockPlanNode(rightEmbeddings, rightInputMetaData);

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftChild, rightChild,
      Collections.singletonList("v1"), MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    List<Embedding> result = node.execute().collect();
    result.sort(Comparator.comparing(o -> o.getProperty(0))); // sort by property value in column 0

    assertThat(result.size(), is(2));
    assertEmbedding(result.get(0), Arrays.asList(b, d, f), Arrays.asList(PropertyValue.create(23), PropertyValue.create(77)));
    assertEmbedding(result.get(1), Arrays.asList(a, c, e), Arrays.asList(PropertyValue.create(42), PropertyValue.create(84)));
  }

  private void assertEmbedding(Embedding e, List<GradoopId> expectedEntries, List<PropertyValue> expectedProperties) {
    expectedEntries.forEach(entry -> assertThat(e.getId(expectedEntries.indexOf(entry)), is(entry)));
    expectedProperties.forEach(value -> assertThat(e.getProperty(expectedProperties.indexOf(value)), is(value)));
  }
}
