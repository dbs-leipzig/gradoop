package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions;


import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions.MergeTemporalEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class MergeTemporalEmbeddingsTest extends PhysicalTPGMOperatorTest {

//------------------------------------------------------------------------------------------------
// Test embedding extensions
//------------------------------------------------------------------------------------------------

    @Test
    public void testEdgeJoinOnRightmostColumn() throws Exception {
        GradoopId v0 = GradoopId.get();
        GradoopId v1 = GradoopId.get();

        GradoopId e0 = GradoopId.get();

        // [Id(v0)]
        EmbeddingTPGM left = createEmbeddingTPGM(v0);

        // [Id(v0),Id(e0),Id(v1)]
        EmbeddingTPGM right = createEmbeddingTPGM(v0, e0, v1);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(3,
                Lists.newArrayList(0),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        // expected: [Id(v0),Id(e0),Id(v1)]
        Assert.assertEquals(createEmbeddingTPGM(v0, e0, v1), result.get(0));
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
        EmbeddingTPGM left = createEmbeddingTPGM(v0, e0, v1, e1, v2);

        // [Id(v1),Id(e2),Id(v3)]
        EmbeddingTPGM right = createEmbeddingTPGM(v1, e2, v3);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(3,
                Lists.newArrayList(0),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3)]
        Assert.assertEquals(createEmbeddingTPGM(v0, e0, v1, e1, v2, e2, v3), result.get(0));
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
        EmbeddingTPGM left = createEmbeddingTPGM(v0, e0, v1, e1, v2);

        // [Id(v3),Id(e2),Id(v1),Id(e3),Id(v4)]
        EmbeddingTPGM right = createEmbeddingTPGM(v3, e2, v1, e3, v4);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(5,
                Lists.newArrayList(2),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(v3),Id(e2),Id(e3),Id(v4)]
        Assert.assertEquals(createEmbeddingTPGM(v0, e0, v1, e1, v2, v3, e2, e3, v4), result.get(0));
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
        EmbeddingTPGM left = createEmbeddingTPGM(v0, e0, v1);

        // [Id(v0),Id(e1),Id(v1)]
        EmbeddingTPGM right = createEmbeddingTPGM(v0, e1, v1);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(3,
                Lists.newArrayList(0, 2),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        // expected: [Id(v0),Id(e0),Id(v1),Id(e1)]
        Assert.assertEquals(createEmbeddingTPGM(v0, e0, v1, e1), result.get(0));
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
        EmbeddingTPGM left = createEmbeddingTPGM(v0, e0, v1, e1, v2);

        // [Id(v0),Id(e2),Id(v3),Id(e3),Id(v2)]
        EmbeddingTPGM right = createEmbeddingTPGM(v0, e2, v3, e3, v2);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(5,
                Lists.newArrayList(0, 4),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(e3)]
        Assert.assertEquals(createEmbeddingTPGM(v0, e0, v1, e1, v2, e2, v3, e3), result.get(0));
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
        EmbeddingTPGM left = createEmbeddingTPGM(v0, e0, v1, e1, v2, e2, v3);

        // [Id(v4),Id(e3),Id(v1),Id(e4),Id(v2),Id(e5),Id(v5)]
        EmbeddingTPGM right = createEmbeddingTPGM(v4, e3, v1, e4, v2, e5, v5);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(7,
                Lists.newArrayList(2, 4),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        // expected: [Id(v0),Id(e0),Id(v1),Id(e1),Id(v2),Id(e2),Id(v3),Id(v4),Id(e3),Id(e4),Id(e5),Id(v5)]
        Assert.assertEquals(createEmbeddingTPGM(v0, e0, v1, e1, v2, e2, v3, v4, e3, e4, e5, v5), result.get(0));
    }

    //------------------------------------------------------------------------------------------------
    // Test column adoption options
    //------------------------------------------------------------------------------------------------

    /**
     * Tests keep all properties and timedata from the left side
     */
    @Test
    public void testAdoptLeft() throws Exception {
        GradoopId v0 = GradoopId.get();
        GradoopId e0 = GradoopId.get();
        GradoopId v1 = GradoopId.get();

        EmbeddingTPGM left = new EmbeddingTPGM();
        left.add(v0, PropertyValue.create("Alice"));
        left.addTimeData(123L, 1234L, 234L, 2345L);

        // [v0, e0, v1]
        EmbeddingTPGM right = createEmbeddingTPGM(v0, e0, v1);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(3,
                Lists.newArrayList(0),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        Assert.assertEquals(PropertyValue.create("Alice"), result.get(0).getProperty(0));
        Assert.assertArrayEquals(new Long[]{123L, 1234L, 234L, 2345L},
                result.get(0).getTimes(0));
    }

    /**
     * Tests keep all properties from the right side
     */
    @Test
    public void testAdoptSameColumn() throws Exception {
        GradoopId v0 = GradoopId.get();
        GradoopId v1 = GradoopId.get();

        // [(Id(v0)]
        EmbeddingTPGM left = createEmbeddingTPGM(v0);

        EmbeddingTPGM right = new EmbeddingTPGM();
        right.add(v0);
        right.add(v1, PropertyValue.create("Alice"));
        right.addTimeData(123L, 1234L, 234L, 2345L);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(2,
                Lists.newArrayList(0),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        Assert.assertEquals(PropertyValue.create("Alice"), result.get(0).getProperty(0));
        Assert.assertArrayEquals(new Long[]{123L, 1234L, 234L, 2345L},
                result.get(0).getTimes(0));
    }

    /**
     * Test keep properties from both sides
     */
    @Test
    public void testAdoptDifferentColumn() throws Exception {
        GradoopId v0 = GradoopId.get();
        GradoopId e0 = GradoopId.get();
        GradoopId v1 = GradoopId.get();

        EmbeddingTPGM left = new EmbeddingTPGM();
        left.add(v0);
        left.add(e0);
        left.add(v1, PropertyValue.create("Alice"));
        left.addTimeData(1L, 2L, 3L, 4L);

        EmbeddingTPGM right = new EmbeddingTPGM();
        right.add(v1, PropertyValue.create(42));
        right.addTimeData(123L, 1234L, 234L, 2345L);

        // join operator
        MergeTemporalEmbeddings udf = new MergeTemporalEmbeddings(1,
                Lists.newArrayList(0),
                Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList()
        );

        List<EmbeddingTPGM> result = new ArrayList<>();

        // get results
        udf.join(left, right, new ListCollector<>(result));

        Assert.assertEquals(PropertyValue.create("Alice"), result.get(0).getProperty(0));
        Assert.assertEquals(PropertyValue.create(42), result.get(0).getProperty(1));

        Assert.assertArrayEquals(new Long[]{1L, 2L, 3L, 4L},
                result.get(0).getTimes(0));
        Assert.assertArrayEquals(new Long[]{123L, 1234L, 234L, 2345L},
                result.get(0).getTimes(1));
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
    // MATCH (vertex0)-[edge0]->(vertex1)<-[edge1]-(vertex0) RETURN *
    //
    // The input embeddings are always
    //
    // 0 0 1                   0 0 1
    // 0 1 1                   0 1 1
    // 0 2 0 |><|(0=0 AND 2=2) 0 2 0
    // 0 3 0                   0 3 0
    //------------------------------------------------------------------------------------------------

    private static GradoopId vertex0 = GradoopId.get();
    private static GradoopId vertex1 = GradoopId.get();
    private static GradoopId edge0 = GradoopId.get();
    private static GradoopId edge1 = GradoopId.get();
    private static GradoopId edge2 = GradoopId.get();
    private static GradoopId edge3 = GradoopId.get();

    /**
     * Tests for vertex homomorphism and edge homomorphism
     * <p>
     * Expected Output:
     * <p>
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
        testMorphisms(emptyList, emptyList, // vertex columns
                emptyList, emptyList,  // edge columns
                Lists.newArrayList(createEmbeddingTPGM(vertex0, edge0, vertex1, edge0), createEmbeddingTPGM(vertex0, edge0, vertex1, edge1),
                        createEmbeddingTPGM(vertex0, edge1, vertex1, edge0), createEmbeddingTPGM(vertex0, edge1, vertex1, edge1),
                        createEmbeddingTPGM(vertex0, edge2, vertex0, edge2), createEmbeddingTPGM(vertex0, edge2, vertex0, edge3),
                        createEmbeddingTPGM(vertex0, edge3, vertex0, edge2), createEmbeddingTPGM(vertex0, edge3, vertex0, edge3)));
    }

    /**
     * Tests for vertex isomorphism and edge homomorphism
     * <p>
     * Expected Output:
     * <p>
     * 0 0 1 0
     * 0 0 1 1
     * 0 1 1 0
     * 0 1 1 1
     */
    @Test
    public void testVertexIsomorphismEdgeHomomorphism() throws Exception {
        List<Integer> emptyList = Collections.emptyList();
        testMorphisms(Arrays.asList(0, 2), emptyList, // vertex columns
                emptyList, emptyList, // edge columns
                Lists.newArrayList(createEmbeddingTPGM(vertex0, edge0, vertex1, edge0), createEmbeddingTPGM(vertex0, edge0, vertex1, edge1),
                        createEmbeddingTPGM(vertex0, edge1, vertex1, edge0), createEmbeddingTPGM(vertex0, edge1, vertex1, edge1)));
    }

    /**
     * Tests for vertex isomorphism and edge homomorphism
     * <p>
     * Expected Output:
     * <p>
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
                Lists.newArrayList(createEmbeddingTPGM(vertex0, edge0, vertex1, edge1), createEmbeddingTPGM(vertex0, edge1, vertex1, edge0),
                        createEmbeddingTPGM(vertex0, edge2, vertex0, edge3), createEmbeddingTPGM(vertex0, edge3, vertex0, edge2)));
    }

    /**
     * Tests for vertex isomorphism and edge homomorphism
     * <p>
     * Expected Output:
     * <p>
     * 0 0 1 1
     * 0 1 1 0
     */
    @Test
    public void testVertexIsomorphismEdgeIsomorphism() throws Exception {
        testMorphisms(Arrays.asList(0, 2), Collections.emptyList(), // vertex columns
                Collections.singletonList(1), Collections.singletonList(1), // edge columns
                Lists.newArrayList(createEmbeddingTPGM(vertex0, edge0, vertex1, edge1), createEmbeddingTPGM(vertex0, edge1, vertex1, edge0)));
    }

    /**
     * Creates the input datasets, performs the join and validates the expected result.
     *
     * @param distinctVertexColumnsLeft join operator argument
     * @param distinctEdgeColumnsLeft   join operator argument
     * @param expectedEmbeddingTPGM         expected result
     * @throws Exception on failure
     */
    private void testMorphisms(List<Integer> distinctVertexColumnsLeft, List<Integer> distinctVertexColumnsRight,
                               List<Integer> distinctEdgeColumnsLeft, List<Integer> distinctEdgeColumnsRight, List<EmbeddingTPGM> expectedEmbeddingTPGM) throws Exception {

        List<EmbeddingTPGM> entries = new ArrayList<>();

        entries.add(createEmbeddingTPGM(vertex0, edge0, vertex1));
        entries.add(createEmbeddingTPGM(vertex0, edge1, vertex1));
        entries.add(createEmbeddingTPGM(vertex0, edge2, vertex0));
        entries.add(createEmbeddingTPGM(vertex0, edge3, vertex0));

        // merge operator
        MergeTemporalEmbeddings op = new MergeTemporalEmbeddings(3, Arrays.asList(0, 2), distinctVertexColumnsLeft,
                distinctVertexColumnsRight, distinctEdgeColumnsLeft, distinctEdgeColumnsRight);

        ArrayList<EmbeddingTPGM> resultList = new ArrayList<>();

        // get results
        for (EmbeddingTPGM left : entries) {
            for (EmbeddingTPGM right : entries) {
                if (left.getId(0).equals(right.getId(0)) && left.getId(2).equals(right.getId(2))) {
                    ArrayList<EmbeddingTPGM> tmp = new ArrayList<>();
                    ListCollector<EmbeddingTPGM> collector = new ListCollector<>(tmp);
                    op.join(left, right, collector);

                    resultList.addAll(tmp.stream().map(EmbeddingTPGM::copy).collect(Collectors.toList()));
                }
            }
        }

        resultList.sort(new EmbeddingTPGMComparator());
        expectedEmbeddingTPGM.sort(new EmbeddingTPGMComparator());

        // test list equality
        assertEquals(expectedEmbeddingTPGM, resultList);
    }


    /**
     * Compares two embeddings based on the contained ids.
     */
    private static class EmbeddingTPGMComparator implements Comparator<EmbeddingTPGM> {

        @Override
        public int compare(EmbeddingTPGM o1, EmbeddingTPGM o2) {
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

    private static EmbeddingTPGM createEmbeddingTPGM(GradoopId... ids) {
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        Arrays.stream(ids).forEach(embedding::add);
        return embedding;
    }
}

