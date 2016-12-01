package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.ProjectionEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // [Id(v0)]
    DataSet<Embedding> left = createEmbeddings(1, new IdEntry(v0));

    // [Id(v0),Id(e0),Id(v1)]
    DataSet<Embedding> right = createEmbeddings(1, new IdEntry(v0), new IdEntry(e0), new IdEntry(v1));

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,0, 0);

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

    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
    DataSet<Embedding> left = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1), new IdEntry(v2));

    // [Id(v1),Id(e2),Id(v3)]
    DataSet<Embedding> right = createEmbeddings(1,
      new IdEntry(v1), new IdEntry(e2), new IdEntry(v3));

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 2, 0);

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


    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
    DataSet<Embedding> left = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1), new IdEntry(v2));

    // [Id(v3),Id(e2),Id(v1),Id(e3),Id(v4)]
    DataSet<Embedding> right = createEmbeddings(1,
      new IdEntry(v3), new IdEntry(e2), new IdEntry(v1), new IdEntry(e3), new IdEntry(v4));

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 2, 2);

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

    // [Id(v0),Id(e0),Id(v1)]
    DataSet<Embedding> left = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1));

    // [Id(v0),Id(e1),Id(v1)]
    DataSet<Embedding> right = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e1), new IdEntry(v1));

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
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

    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
    DataSet<Embedding> left = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1), new IdEntry(v2));

    // [Id(v0),Id(e2),Id(v3),Id(e3),Id(v2)]
    DataSet<Embedding> right = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e2), new IdEntry(v3), new IdEntry(e3), new IdEntry(v2));

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
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

    // [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
    DataSet<Embedding> left = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1),
      new IdEntry(v2), new IdEntry(e2), new IdEntry(v3));

    // [Id(v4),Id(e3),Id(v1),Id(e4),Id(v2),Id(e5),Id(v5)]
    DataSet<Embedding> right = createEmbeddings(1,
      new IdEntry(v4), new IdEntry(e3), new IdEntry(v1), new IdEntry(e4),
      new IdEntry(v2), new IdEntry(e5), new IdEntry(v5));

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
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
   * Tests the adoption of the left side columns
   *
   * [Projection(v0,{name:Alice})]
   * |><|(0=0)
   * [(Id(v0),Id(e0),Id(v1)]
   * ->
   * [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
   */
  @Test
  public void testAdoptLeft() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    // [Projection(v0,{name:Alice})]
    Properties properties = Properties.create();
    properties.set("name", "Alice");
    DataSet<Embedding> left = createEmbeddings(1, new ProjectionEntry(v0, properties));

    // [Id(v0),Id(e0),Id(v1)]
    DataSet<Embedding> right = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1));

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,0, 0);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Assert.assertEquals(new Embedding(Lists.newArrayList(
      new ProjectionEntry(v0, properties),
      new IdEntry(e0),
      new IdEntry(v1))),
      result);
  }

  /**
   * Tests the adoption of the right side columns
   *
   * [(Id(v0)]
   * |><|(0=0)
   * [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
   * ->
   * [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
   */
  @Test
  public void testAdoptSameColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    // [(Id(v0)]
    DataSet<Embedding> left = createEmbeddings(1, new IdEntry(v0));

    // [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Properties properties = Properties.create();
    properties.set("name", "Alice");
    DataSet<Embedding> right = createEmbeddings(1,
      new ProjectionEntry(v0, properties), new IdEntry(e0), new IdEntry(v1));

    Map<Integer, Integer> adoptColumns = new HashMap<>(0);
    adoptColumns.put(0, 0);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right, 0, 0, adoptColumns);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Assert.assertEquals(new Embedding(Lists.newArrayList(
      new ProjectionEntry(v0, properties),
      new IdEntry(e0),
      new IdEntry(v1))),
      result);
  }

  /**
   * Tests the adoption of the right side columns
   *
   * [Id(v0),Id(e0),Id(v1)]
   * |><|(2=0)
   * [Projection(v1,{name:Alice})]
   * ->
   * [Id(v0),Id(e0),Projection(v1,{name:Alice})]
   */
  @Test
  public void testAdoptDifferentColumn() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    // [(Id(v0)]
    DataSet<Embedding> left = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1));

    // [Id(v0),Id(e0),Projection(v1,{name:Alice})]
    Properties properties = Properties.create();
    properties.set("name", "Alice");
    DataSet<Embedding> right = createEmbeddings(1, new ProjectionEntry(v1, properties));

    Map<Integer, Integer> adoptColumns = new HashMap<>(0);
    adoptColumns.put(0, 2);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,2, 0, adoptColumns);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Id(v0),Id(e0),Projection(v1,{name:Alice})]
    Assert.assertEquals(new Embedding(Lists.newArrayList(
      new IdEntry(v0),
      new IdEntry(e0),
      new ProjectionEntry(v1, properties))),
      result);
  }

  /**
   * Tests the adoption of left and right side columns
   *
   * [(Id(v0),Id(e0),Projection(v1,{name:Alice})]
   * |><|(0=0 AND 2=2)
   * [Projection(v0,{name:Bob}),Id(e1),Id(v1)]
   * ->
   * [Projection(v0,{name:Bob}),Id(e0),Projection(v1,{name:Alice}),Id(e1)]
   */
  @Test
  public void testAdoptLeftAndRight() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();
    GradoopId e1 = GradoopId.get();

    // [(Id(v0),Id(e0),Projection(v1,{name:Alice})]
    Properties propertiesLeft = Properties.create();
    propertiesLeft.set("name", "Alice");
    DataSet<Embedding> left = createEmbeddings(1,
      new IdEntry(v0), new IdEntry(e0), new ProjectionEntry(v1, propertiesLeft));

    // [Projection(v0,{name:Bob}),Id(e1),Id(v1)]
    Properties propertiesRight = Properties.create();
    propertiesLeft.set("name", "Bob");
    DataSet<Embedding> right = createEmbeddings(1,
      new ProjectionEntry(v0, propertiesRight), new IdEntry(e1), new IdEntry(v1));

    Map<Integer, Integer> adoptColumns = new HashMap<>(0);
    adoptColumns.put(0, 0);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      Arrays.asList(0, 2), Arrays.asList(0, 2),
      adoptColumns);

    // get results
    Embedding result = join.evaluate().collect().get(0);

    // expected: [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Assert.assertEquals(new Embedding(Lists.newArrayList(
      new ProjectionEntry(v0, propertiesRight),
      new IdEntry(e0),
      new ProjectionEntry(v1, propertiesLeft),
      new IdEntry(e1))),
      result);
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
    testMorphisms(emptyList, emptyList, // vertex columns
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
    PhysicalOperator join = new JoinEmbeddings(left, right,
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
        if (o1.getEntry(i).getId().compareTo(o2.getEntry(i).getId()) < 0) {
          return -1;
        } else if (o1.getEntry(i).getId().compareTo(o2.getEntry(i).getId()) > 0) {
          return 1;
        }
      }
      return 0;
    }
  }
}
