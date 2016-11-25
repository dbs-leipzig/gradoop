package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JoinEmbeddingsTest extends PhysicalOperatorTest {

  private static Embedding createEmbedding(GradoopId... ids) {
    Embedding embedding = new Embedding();

    for (GradoopId id : ids) {
      embedding.addEntry(new IdEntry(id));
    }

    return embedding;
  }

  private DataSet<Embedding> createEmbeddings(int size, List<EmbeddingEntry> entries) {
    List<Embedding> embeddings = new ArrayList<>(size);

    for (int i = 0; i < size; i++) {
      embeddings.add(new Embedding(entries));
    }

    return getExecutionEnvironment().fromCollection(embeddings);
  }

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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(new IdEntry(v0));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Id(v0),Id(e0),Id(v1)]
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {0}, // join columns left
      new int[] {0}, // join columns right
      new int[] {0}, // adopt columns left
      new int[] {},  // adopt columns right
      new int[] {},  // distinct vertex columns
      new int[] {},  // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Id(v0),Id(e0),Id(v1)]
    Assert.assertEquals(Lists.newArrayList(rightEntries), resultList);
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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1), new IdEntry(v2));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Id(v1),Id(e2),Id(v3)]
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new IdEntry(v1), new IdEntry(e2), new IdEntry(v3));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {2}, // join columns left
      new int[] {0}, // join columns right
      new int[] {2}, // adopt columns left
      new int[] {},  // adopt columns right
      new int[] {},  // distinct vertex columns
      new int[] {},  // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
      Lists.newArrayList(
        new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1),
        new IdEntry(v2), new IdEntry(e2), new IdEntry(v3)))),
      resultList);
  }

  /**
   * Tests joining embeddings on intermediate positions.
   *
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
   * |><|(2=0)
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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1), new IdEntry(v2));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Id(v3),Id(e2),Id(v1),Id(e3),Id(v4)]
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new IdEntry(v3), new IdEntry(e2), new IdEntry(v1), new IdEntry(e3), new IdEntry(v4));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {2}, // join columns left
      new int[] {2}, // join columns right
      new int[] {2}, // adopt columns left
      new int[] {},  // adopt columns right
      new int[] {},  // distinct vertex columns
      new int[] {},  // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(v3),Id(e2),Id(e3),Id(v4)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
        Lists.newArrayList(
          new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1), new IdEntry(v2),
          new IdEntry(v3), new IdEntry(e2), new IdEntry(e3), new IdEntry(v4)))),
      resultList);
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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Id(v0),Id(e1),Id(v1)]
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e1), new IdEntry(v1));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {0,2}, // join columns left
      new int[] {0,2}, // join columns right
      new int[] {0,2}, // adopt columns left
      new int[] {},    // adopt columns right
      new int[] {},    // distinct vertex columns
      new int[] {},    // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
        Lists.newArrayList(new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1)))),
      resultList);
  }

  /**
   * Tests joining edge embeddings on two columns.
   *
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2)]
   * |><|(0=0 AND 2=2)
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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1), new IdEntry(v2));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Id(v0),Id(e2),Id(v3),Id(e3),Id(v2)]
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e2), new IdEntry(v3), new IdEntry(e3), new IdEntry(v2));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {0,4}, // join columns left
      new int[] {0,4}, // join columns right
      new int[] {0,4}, // adopt columns left
      new int[] {},    // adopt columns right
      new int[] {},    // distinct vertex columns
      new int[] {},    // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(e3)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
        Lists.newArrayList(
          new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1),
          new IdEntry(v2), new IdEntry(e2), new IdEntry(v3), new IdEntry(e3)))),
      resultList);
  }

  /**
   * Tests joining edge embeddings on two mid-columns.
   *
   * [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
   * |><|(0=0 AND 2=2)
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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e0),
      new IdEntry(v1), new IdEntry(e1),
      new IdEntry(v2), new IdEntry(e2), new IdEntry(v3));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Id(v4),Id(e3),Id(v1),Id(e4),Id(v2),Id(e5),Id(v5)]
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new IdEntry(v4), new IdEntry(e3),
      new IdEntry(v1), new IdEntry(e4),
      new IdEntry(v2), new IdEntry(e5), new IdEntry(v5));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {2,4}, // join columns left
      new int[] {2,4}, // join columns right
      new int[] {2,4}, // adopt columns left
      new int[] {},    // adopt columns right
      new int[] {},    // distinct vertex columns
      new int[] {},    // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(v4),Id(e3),Id(e4),Id(e5),Id(v5)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
        Lists.newArrayList(
          new IdEntry(v0), new IdEntry(e0), new IdEntry(v1), new IdEntry(e1),
          new IdEntry(v2), new IdEntry(e2), new IdEntry(v3), new IdEntry(v4),
          new IdEntry(e3), new IdEntry(e4), new IdEntry(e5), new IdEntry(v5)))),
      resultList);
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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(new ProjectionEntry(v0, properties));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Id(v0),Id(e0),Id(v1)]
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new IdEntry(v0), new IdEntry(e0), new IdEntry(v1));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {0}, // join columns left
      new int[] {0}, // join columns right
      new int[] {0}, // adopt columns left
      new int[] {},  // adopt columns right
      new int[] {},  // distinct vertex columns
      new int[] {},  // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
      Lists.newArrayList(
        new ProjectionEntry(v0, properties),
        new IdEntry(e0),
        new IdEntry(v1)))),
      resultList);
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
  public void testAdoptRight() throws Exception {
    GradoopId v0 = GradoopId.get();
    GradoopId e0 = GradoopId.get();
    GradoopId v1 = GradoopId.get();

    // [(Id(v0)]
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(new IdEntry(v0));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Properties properties = Properties.create();
    properties.set("name", "Alice");
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new ProjectionEntry(v0, properties), new IdEntry(e0), new IdEntry(v1));
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {0}, // join columns left
      new int[] {0}, // join columns right
      new int[] {},  // adopt columns left
      new int[] {0}, // adopt columns right
      new int[] {},  // distinct vertex columns
      new int[] {},  // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
        Lists.newArrayList(
          new ProjectionEntry(v0, properties),
          new IdEntry(e0),
          new IdEntry(v1)))),
      resultList);
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
    List<EmbeddingEntry> leftEntries = Lists.newArrayList(
      new IdEntry(v0),
      new IdEntry(e0),
      new ProjectionEntry(v1, propertiesLeft));
    DataSet<Embedding> left = createEmbeddings(1, leftEntries);

    // [Projection(v0,{name:Bob}),Id(e1),Id(v1)]
    Properties propertiesRight = Properties.create();
    propertiesLeft.set("name", "Bob");
    List<EmbeddingEntry> rightEntries = Lists.newArrayList(
      new ProjectionEntry(v0, propertiesRight),
      new IdEntry(e1),
      new IdEntry(v1)
    );
    DataSet<Embedding> right = createEmbeddings(1, rightEntries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {0,2}, // join columns left
      new int[] {0,2}, // join columns right
      new int[] {2},   // adopt columns left
      new int[] {0},   // adopt columns right
      new int[] {},    // distinct vertex columns
      new int[] {},    // distinct edge columns
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // expected: [Projection(v0,{name:Alice}),Id(e0),Id(v1)]
    Assert.assertEquals(Lists.newArrayList(new Embedding(
        Lists.newArrayList(
          new ProjectionEntry(v0, propertiesRight),
          new IdEntry(e0),
          new ProjectionEntry(v1, propertiesLeft),
          new IdEntry(e1)))),
      resultList);
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

  private static List<Embedding> entries;

  static {
    entries = new ArrayList<>();
    entries.add(createEmbedding(v0, e0, v1));
    entries.add(createEmbedding(v0, e1, v1));
    entries.add(createEmbedding(v0, e2, v0));
    entries.add(createEmbedding(v0, e3, v0));
  }

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
    testMorphisms(new int[0], new int[0], Lists.newArrayList(
      createEmbedding(v0, e0, v1, e0),
      createEmbedding(v0, e0, v1, e1),
      createEmbedding(v0, e1, v1, e0),
      createEmbedding(v0, e1, v1, e1),
      createEmbedding(v0, e2, v0, e2),
      createEmbedding(v0, e2, v0, e3),
      createEmbedding(v0, e3, v0, e2),
      createEmbedding(v0, e1, v1, e3)));
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
    testMorphisms(new int[] {0, 2}, new int[0], Lists.newArrayList(
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
    testMorphisms(new int[0], new int[] {1, 2}, Lists.newArrayList(
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
    testMorphisms(new int[] {0, 2}, new int[] {1, 3}, Lists.newArrayList(
      createEmbedding(v0, e0, v1, e1),
      createEmbedding(v0, e1, v1, e0)));
  }

  /**
   * Creates the input datasets, performs the join and validates the expected result.
   *
   * @param distinctVertexColumns join operator argument
   * @param distinctEdgeColumns join operator argument
   * @param expectedEmbedding expected result
   * @throws Exception
   */
  private void testMorphisms(int[] distinctVertexColumns, int[] distinctEdgeColumns,
    List<Embedding> expectedEmbedding) throws Exception {

    DataSet<Embedding> left = getExecutionEnvironment().fromCollection(entries);
    DataSet<Embedding> right = getExecutionEnvironment().fromCollection(entries);

    // join operator
    PhysicalOperator join = new JoinEmbeddings(left, right,
      new int[] {0, 2},
      new int[] {0, 2},
      new int[] {0, 2},
      new int[] {},
      distinctVertexColumns,
      distinctEdgeColumns,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);

    // get results
    List<Embedding> resultList = join.evaluate().collect();

    // test list equality
    Assert.assertEquals(expectedEmbedding, resultList);
  }
}
