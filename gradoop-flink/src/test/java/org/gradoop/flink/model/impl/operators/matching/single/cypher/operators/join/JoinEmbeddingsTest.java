package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.createEmbedding;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.createEmbeddings;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class JoinEmbeddingsTest extends PhysicalOperatorTest {

  //------------------------------------------------------------------------------------------------
  // Test embedding extensions
  //------------------------------------------------------------------------------------------------

  /**
   * Tests the extension of a vertex embedding with an edge embedding.
   *
   * [Id(v0)]
   * |><|(0=0)
   * [Id(v0),Id(e0),Id(v1)]
   * ->
   * [Id(v0),Id(e0),Id(v1)]
   */
  @Test
  public void testEdgeJoinOnRightmostColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    GradoopId e0 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    // [Id(v0)]
    DataSet<Embedding> left = createEmbeddings(env, 1,v0);

    // [Id(v0),Id(e0),Id(v1)]
    DataSet<Embedding> right = createEmbeddings(env, 1, v0, e0, v1);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 3, 0, 0);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Id(v0),Id(e0),Id(v1)]
    Assert.assertEquals(createEmbedding(v0, e0, v1), result);
  }

  /**
   * Tests joining embeddings on intermediate positions.
   *
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
   * |><|(2=0)
   * [Id(v1),Id(e2),Id(v3)]
   * ->
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
   */
  @Test
  public void testEdgeJoinOnMidColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    GradoopId v3 = GradoopId.get();

    GradoopId e0 = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId e2 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
    DataSet<Embedding> left = createEmbeddings(env, 1, v0, e0, v1, e1, v2);

    // [Id(v1),Id(e2),Id(v3)]
    DataSet<Embedding> right = createEmbeddings(env, 1, v1, e2 ,v3);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 3, 2, 0);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
    Assert.assertEquals(createEmbedding(v0, e0, v1, e1, v2, e2, v3), result);
  }

  /**
   * Tests joining embeddings on intermediate positions.
   *
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
   * |><|(2=2)
   * [Id(v3),Id(e2),Id(v1),Id(e3),Id(v4)]
   * ->
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(v3),Id(e2),Id(e3),Id(v4)]
   */
  @Test
  public void testPathJoinOnMidColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    GradoopId v3 = GradoopId.get();
    GradoopId v4 = GradoopId.get();

    GradoopId e0 = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId e3 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
    DataSet<Embedding> left = createEmbeddings(env, 1, v0, e0, v1, e1, v2);

    // [Id(v3),Id(e2),Id(v1),Id(e3),Id(v4)]
    DataSet<Embedding> right = createEmbeddings(env, 1, v3, e2, v1, e3, v4);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 5, 2, 2);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(v3),Id(e2),Id(e3),Id(v4)]
    Assert.assertEquals(createEmbedding(v0, e0, v1, e1, v2, v3, e2, e3, v4), result);
  }

  /**
   * Tests joining edge embeddings on two columns.
   *
   * [Id(v0),Id(e0),Id(v1)]
   * |><|(0=0 AND 2=2)
   * [Id(v0),Id(e1),Id(v1)]
   * ->
   * [Id(v0),Id(e0),Id(v1),Id(e1)]
   */
  @Test
  public void testEdgeJoinOnBoundaryColumns() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId e1 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    // [Id(v0),Id(e0),Id(v1)]
    DataSet<Embedding> left = createEmbeddings(env, 1, v0, e0, v1);

    // [Id(v0),Id(e1),Id(v1)]
    DataSet<Embedding> right = createEmbeddings(env, 1, v0, e1, v1);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 3,
      Arrays.asList(0, 2), Arrays.asList(0, 2));

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1)]
    Assert.assertEquals(createEmbedding(v0, e0, v1, e1), result);
  }

  /**
   * Tests joining edge embeddings on two columns.
   *
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
   * |><|(0=0 AND 4=4)
   * [Id(v0),Id(e2),Id(v3),Id(e3),Id(v2)]
   * ->
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(e3)]
   */
  @Test
  public void testPathJoinOnBoundaryColumns() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    GradoopId v3 = GradoopId.get();

    GradoopId e0 = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId e3 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
    DataSet<Embedding> left = createEmbeddings(env, 1,
      v0, e0, v1, e1, v2);

    // [Id(v0),Id(e2),Id(v3),Id(e3),Id(v2)]
    DataSet<Embedding> right = createEmbeddings(env, 1,
      v0, e2, v3, e3, v2);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 5,
      Arrays.asList(0, 4), Arrays.asList(0, 4));

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(e3)]
    Assert.assertEquals(createEmbedding(v0, e0, v1, e1, v2, e2, v3, e3), result);
  }

  /**
   * Tests joining edge embeddings on two mid-columns.
   *
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
   * |><|(2=2 AND 4=4)
   * [Id(v4),Id(e3),Id(v1),Id(e4),Id(v2),Id(e5),Id(v5)]
   * ->
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(v4),Id(e3),Id(e4),Id(e5),Id(v5)]
   */
  @Test
  public void testPathJoinOnMidColumns() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    GradoopId v3 = GradoopId.get();
    GradoopId v4 = GradoopId.get();
    GradoopId v5 = GradoopId.get();

    GradoopId e0 = GradoopId.get();
    GradoopId e1 = GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId e3 = GradoopId.get();
    GradoopId e4 = GradoopId.get();
    GradoopId e5 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
    DataSet<Embedding> left = createEmbeddings(env, 1, v0, e0, v1, e1, v2, e2, v3);

    // [Id(v4),Id(e3),Id(v1),Id(e4),Id(v2),Id(e5),Id(v5)]
    DataSet<Embedding> right = createEmbeddings(env, 1, v4, e3, v1, e4, v2, e5, v5);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 7,
      Arrays.asList(2, 4), Arrays.asList(2, 4));

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(v4),Id(e3),Id(e4),Id(e5),Id(v5)]
    Assert.assertEquals(createEmbedding(v0, e0, v1, e1, v2, e2, v3, v4, e3, e4, e5, v5), result);
  }

  //------------------------------------------------------------------------------------------------
  // Test column adoption options
  //------------------------------------------------------------------------------------------------

  /**
   * Tests keep all properties from the left side
   */
  @Test
  public void testAdoptLeft() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    Embedding embedding = new Embedding();
    embedding.add(v0, PropertyValue.create("Alice"));
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(embedding);

    // [v0, e0, v1]
    DataSet<Embedding> right = createEmbeddings(env, 1, v0, e0, v1);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 3,0, 0);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    Assert.assertEquals(PropertyValue.create("Alice"), result.getProperty(0));
  }

  /**
   * Tests keep all properties from the right side
   */
  @Test
  public void testAdoptSameColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    ExecutionEnvironment env = getExecutionEnvironment();

    // [(Id(v0)]
    DataSet<Embedding> left = createEmbeddings(env, 1, v0);

    Embedding embedding = new Embedding();
    embedding.add(v0);
    embedding.add(v1, PropertyValue.create("Alice"));
    DataSet<Embedding> right = getExecutionEnvironment().fromElements(embedding);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 1, 0, 0);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    Assert.assertEquals(PropertyValue.create("Alice"), result.getProperty(0));
  }

  /**
   * Test keep properties from both sides
   */
  @Test
  public void testAdoptDifferentColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    Embedding embeddingLeft = new Embedding();
    embeddingLeft.add(v0);
    embeddingLeft.add(e0);
    embeddingLeft.add(v1, PropertyValue.create("Alice"));
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(embeddingLeft);

    Embedding embeddingRight = new Embedding();
    embeddingRight.add(v1, PropertyValue.create(42));
    DataSet<Embedding> right = getExecutionEnvironment().fromElements(embeddingRight);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 1, 2, 0);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    Assert.assertEquals(PropertyValue.create("Alice"), result.getProperty(0));
    Assert.assertEquals(PropertyValue.create(42), result.getProperty(1));
  }

  //------------------------------------------------------------------------------------------------
  // Test Isomorphism / Homomorphism options
  //
  // Data graph uses throughout the tests
  //
  // CREATE
  // (a{id:0}),(b{id:1}),
  // (a)-[:LINK{id:0}]->(b),
  // (a)-[:LINK{id:1}]->(b),
  // (a)-[:LINK{id:2}]->(a),
  // (a)-[:LINK{id:3}]->(a)
  //
  // Query graph used throughout the tests
  //
  // MATCH (v0)-[e0]->(v1)<-[e1]-(v0) RETURN *
  //
  // The input embeddings are always
  //
  // 0 0 1                   0 0 1
  // 0 1 1                   0 1 1
  // 0 2 0 |><|(0=0 AND 2=2) 0 2 0
  // 0 3 0                   0 3 0
  //------------------------------------------------------------------------------------------------

  private static GradoopId v0 = GradoopId.get();
  private static GradoopId v1 = GradoopId.get();
  private static GradoopId e0 = GradoopId.get();
  private static GradoopId e1 = GradoopId.get();
  private static GradoopId e2 = GradoopId.get();
  private static GradoopId e3 = GradoopId.get();

  /**
   * Tests for vertex homomorphism and edge homomorphism
   *
   * Expected Output:
   *
   * 0 0 1 0
   * 0 0 1 1
   * 0 1 1 0
   * 0 1 1 1
   * 0 2 0 2
   * 0 2 0 3
   * 0 3 0 2
   * 0 3 0 3
   */
  @Test
  public void testVertexHomomorphismEdgeHomomorphism() throws Exception {
    List<Integer> emptyList = Collections.emptyList();
    testMorphisms(
      emptyList, emptyList, // vertex columns
      emptyList, emptyList,  // edge columns
      Lists.newArrayList(
        createEmbedding(v0, e0, v1, e0),
        createEmbedding(v0, e0, v1, e1),
        createEmbedding(v0, e1, v1, e0),
        createEmbedding(v0, e1, v1, e1),
        createEmbedding(v0, e2, v0, e2),
        createEmbedding(v0, e2, v0, e3),
        createEmbedding(v0, e3, v0, e2),
        createEmbedding(v0, e3, v0, e3)));
  }

  /**
   * Tests for vertex isomorphism and edge homomorphism
   *
   * Expected Output:
   *
   * 0 0 1 0
   * 0 0 1 1
   * 0 1 1 0
   * 0 1 1 1
   */
  @Test
  public void testVertexIsomorphismEdgeHomomorphism() throws Exception {
    List<Integer> emptyList = Collections.emptyList();
    testMorphisms(
      Arrays.asList(0, 2), emptyList, // vertex columns
      emptyList, emptyList, // edge columns
      Lists.newArrayList(
        createEmbedding(v0, e0, v1, e0),
        createEmbedding(v0, e0, v1, e1),
        createEmbedding(v0, e1, v1, e0),
        createEmbedding(v0, e1, v1, e1)));
  }

  /**
   * Tests for vertex isomorphism and edge homomorphism
   *
   * Expected Output:
   *
   * 0 0 1 1
   * 0 1 1 0
   * 0 2 0 3
   * 0 3 0 2
   */
  @Test
  public void testVertexHomomorphismEdgeIsomorphism() throws Exception {
    List<Integer> emptyList = Collections.emptyList();
    testMorphisms(
      emptyList, emptyList, // vertex columns
      Collections.singletonList(1), Collections.singletonList(1), // edge columns
      Lists.newArrayList(
        createEmbedding(v0, e0, v1, e1),
        createEmbedding(v0, e1, v1, e0),
        createEmbedding(v0, e2, v0, e3),
        createEmbedding(v0, e3, v0, e2)));
  }

  /**
   * Tests for vertex isomorphism and edge homomorphism
   *
   * Expected Output:
   *
   * 0 0 1 1
   * 0 1 1 0
   */
  @Test
  public void testVertexIsomorphismEdgeIsomorphism() throws Exception {
    testMorphisms(
      Arrays.asList(0, 2), Collections.emptyList(), // vertex columns
      Collections.singletonList(1), Collections.singletonList(1), // edge columns
      Lists.newArrayList(
        createEmbedding(v0, e0, v1, e1),
        createEmbedding(v0, e1, v1, e0)));
  }

  /**
   * Creates the input datasets, performs the join and validates the expected result.
   *
   * @param distinctVertexColumnsLeft join operator argument
   * @param distinctEdgeColumnsLeft join operator argument
   * @param expectedEmbedding expected result
   * @throws Exception
   */
  private void testMorphisms(
    List<Integer> distinctVertexColumnsLeft,
    List<Integer> distinctVertexColumnsRight,
    List<Integer> distinctEdgeColumnsLeft,
    List<Integer> distinctEdgeColumnsRight,
    List<Embedding> expectedEmbedding) throws Exception {

    List<Embedding> entries = new ArrayList<>();

    entries.add(createEmbedding(v0, e0, v1));
    entries.add(createEmbedding(v0, e1, v1));
    entries.add(createEmbedding(v0, e2, v0));
    entries.add(createEmbedding(v0, e3, v0));

    DataSet<Embedding> left = getExecutionEnvironment().fromCollection(entries);
    DataSet<Embedding> right = getExecutionEnvironment().fromCollection(entries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 3,
      Arrays.asList(0, 2), Arrays.asList(0, 2),
      distinctVertexColumnsLeft, distinctVertexColumnsRight,
      distinctEdgeColumnsLeft, distinctEdgeColumnsRight);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    resultList.sort(new EmbeddingComparator());
    expectedEmbedding.sort(new EmbeddingComparator());

    // test list equality
    Assert.assertEquals(expectedEmbedding, resultList);
  }

  /**
   * Compares two embeddings based on the contained ids.
   */
  private static class EmbeddingComparator implements Comparator<Embedding> {

    @Override
    public int compare(Embedding o1, Embedding o2) {
      for (int i = 0; i < o1.size(); i++) {
        if (o1.getId(i).compareTo(o2.getId(i)) < 0) {
          return -1;
        } else if (o1.getId(i).compareTo(o2.getId(i)) > 0) {
          return 1;
        }
      }
      return 0;
    }
  }
}
