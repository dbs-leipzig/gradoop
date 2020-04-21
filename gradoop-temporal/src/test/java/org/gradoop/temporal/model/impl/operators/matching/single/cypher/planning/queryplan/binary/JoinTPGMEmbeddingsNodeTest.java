package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinTPGMEmbeddingsNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.gradoop.common.GradoopTestUtils.call;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEmbeddingTPGM;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JoinTPGMEmbeddingsNodeTest {
    @Test
    public void testMetaDataInitialization() throws Exception {
        EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
        leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        leftInputMetaData.setPropertyColumn("v1", "age", 0);
        leftInputMetaData.setPropertyColumn("e1", "since", 1);
        leftInputMetaData.setTimeColumn("v1", 0);
        leftInputMetaData.setTimeColumn("e1", 1);
        leftInputMetaData.setTimeColumn("v2", 0);

        EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
        rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e2", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setPropertyColumn("v2", "age", 0);
        rightInputMetaData.setPropertyColumn("e2", "since", 1);
        rightInputMetaData.setPropertyColumn("v3", "age", 2);
        rightInputMetaData.setTimeColumn("v2", 0);
        rightInputMetaData.setTimeColumn("e2", 1);
        rightInputMetaData.setTimeColumn("v3", 2);

        MockPlanNode leftMockNode = new MockPlanNode(null, leftInputMetaData);
        MockPlanNode rightMockNode = new MockPlanNode(null, rightInputMetaData);

        JoinTPGMEmbeddingsNode node = new JoinTPGMEmbeddingsNode(leftMockNode, rightMockNode,
                singletonList("v2"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        EmbeddingTPGMMetaData outputMetaData = node.getEmbeddingMetaData();

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

        assertThat(outputMetaData.getTimeCount(), is(5));
        assertThat(outputMetaData.getTimeColumn("v1"), is(0));
        assertThat(outputMetaData.getTimeColumn("e1"), is(1));
        assertThat(outputMetaData.getTimeColumn("v2"), is(3));
        assertThat(outputMetaData.getTimeColumn("e2"), is(4));
        assertThat(outputMetaData.getTimeColumn("v3"), is(5));
    }

    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testGetJoinColumns() throws Exception {
        EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
        leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        leftInputMetaData.setTimeColumn("v1", 0);
        leftInputMetaData.setTimeColumn("e1", 1);
        leftInputMetaData.setTimeColumn("v2", 2);

        EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
        rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e2", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setTimeColumn("v2", 0);
        rightInputMetaData.setTimeColumn("e2", 1);
        rightInputMetaData.setTimeColumn("v3", 2);

        PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
        PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

        JoinTPGMEmbeddingsNode node = new JoinTPGMEmbeddingsNode(leftChild, rightChild,
                singletonList("v2"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getJoinColumnsLeft"), is(asList(2)));
        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getJoinColumnsRight"), is(asList(0)));
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
        rightInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e3", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v4", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setEntryColumn("e4", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
        rightInputMetaData.setEntryColumn("v5", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
        rightInputMetaData.setTimeColumn("v3", 0);
        rightInputMetaData.setTimeColumn("e3", 1);
        rightInputMetaData.setTimeColumn("v4", 2);
        rightInputMetaData.setTimeColumn("e4", 3);
        rightInputMetaData.setTimeColumn("v5", 4);

        PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
        PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

        JoinTPGMEmbeddingsNode node = new JoinTPGMEmbeddingsNode(leftChild, rightChild,
                singletonList("v3"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctVertexColumnsLeft"), is(asList(0, 2, 4)));
        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctVertexColumnsRight"), is(asList(2, 4)));
        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList(1, 3)));
        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctEdgeColumnsRight"), is(asList(1, 3)));
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
        rightInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e3", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v4", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setEntryColumn("e4", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
        rightInputMetaData.setEntryColumn("v5", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
        rightInputMetaData.setTimeColumn("v3", 0);
        rightInputMetaData.setTimeColumn("e3", 1);
        rightInputMetaData.setTimeColumn("v4", 2);
        rightInputMetaData.setTimeColumn("e4", 3);
        rightInputMetaData.setTimeColumn("v5", 4);

        PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
        PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

        JoinTPGMEmbeddingsNode node = new JoinTPGMEmbeddingsNode(leftChild, rightChild,
                singletonList("v3"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctVertexColumnsLeft"), is(asList()));
        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctVertexColumnsRight"), is(asList()));
        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList()));
        assertThat(call(JoinTPGMEmbeddingsNode.class, node, "getDistinctEdgeColumnsRight"), is(asList()));
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

        Long[] aTime = new Long[]{1L, 2L, 3L, 4L};
        Long[] bTime = new Long[]{11L, 12L, 13L, 14L};
        Long[] cTime = new Long[]{21L, 22L, 23L, 24L};
        Long[] dTime = new Long[]{31L, 32L, 33L, 34L};
        Long[] eTime = new Long[]{41L, 42L, 43L, 44L};
        Long[] fTime = new Long[]{51L, 52L, 53L, 54L};

        EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
        leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        leftInputMetaData.setPropertyColumn("v1", "age", 0);
        leftInputMetaData.setTimeColumn("v1", 0);

        EmbeddingTPGM embedding1 = createEmbeddingTPGM(new GradoopId[]{a}, new Long[][]{aTime});
        embedding1.addPropertyValues(PropertyValue.create(42));

        EmbeddingTPGM embedding2 = createEmbeddingTPGM(new GradoopId[]{b}, new Long[][]{bTime});
        embedding2.addPropertyValues(PropertyValue.create(23));

        DataSet<EmbeddingTPGM> leftEmbeddings = env.fromElements(embedding1, embedding2);


         /* ----------------------------------------------------------------------------
         * |  v1   | e1    | v2    | v2.age |  v1Time  |  e1Time  |  v2Time  |
         * ---------------------------------------------------------------------------
         * | id(a) | id(c) | id(e) |  84    |  aTime   |  cTime   |  eTime  | -> Embedding 3
         * ---------------------------------------------------------------------------
         * | id(b) | id(d) | id(f) |  77    | bTime   |  dTime   |  fTime  | -> Embedding 3
         * --------------------------------------------------------------------------
        */

        EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
        rightInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setPropertyColumn("v2", "age", 0);
        rightInputMetaData.setTimeColumn("v1", 0);
        rightInputMetaData.setTimeColumn("e1", 1);
        rightInputMetaData.setTimeColumn("v2", 2);

        EmbeddingTPGM embedding3 = createEmbeddingTPGM(new GradoopId[]{a,c,e},
                new Long[][]{aTime, cTime, eTime});
        embedding3.addPropertyValues(PropertyValue.create(84));

        EmbeddingTPGM embedding4 = createEmbeddingTPGM(new GradoopId[]{b,d,f},
                new Long[][]{bTime, dTime, fTime});
        embedding4.addPropertyValues(PropertyValue.create(77));

        DataSet<EmbeddingTPGM> rightEmbeddings = env.fromElements(embedding3, embedding4);

        MockPlanNode leftChild = new MockPlanNode(leftEmbeddings, leftInputMetaData);
        MockPlanNode rightChild = new MockPlanNode(rightEmbeddings, rightInputMetaData);

        JoinTPGMEmbeddingsNode node = new JoinTPGMEmbeddingsNode(leftChild, rightChild,
                singletonList("v1"), MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        List<EmbeddingTPGM> result = node.execute().collect();
        result.sort(Comparator.comparing(o -> o.getProperty(0))); // sort by property value in column 0

        assertThat(result.size(), is(2));

        assertEquals(result.get(1).getId(0), a);
        assertEquals(result.get(1).getId(1), c);
        assertEquals(result.get(1).getId(2), e);
        assertEquals(result.get(1).getProperty(0), PropertyValue.create(42));
        assertEquals(result.get(1).getProperty(1), PropertyValue.create(84));
        assertArrayEquals(result.get(1).getTimes(0), aTime);
        assertArrayEquals(result.get(1).getTimes(1), aTime);
        assertArrayEquals(result.get(1).getTimes(2), cTime);
        assertArrayEquals(result.get(1).getTimes(3), eTime);

        assertEquals(result.get(0).getId(0), b);
        assertEquals(result.get(0).getId(1), d);
        assertEquals(result.get(0).getId(2), f);
        assertEquals(result.get(0).getProperty(0), PropertyValue.create(23));
        assertEquals(result.get(0).getProperty(1), PropertyValue.create(77));
        assertArrayEquals(result.get(0).getTimes(0), bTime);
        assertArrayEquals(result.get(0).getTimes(1), bTime);
        assertArrayEquals(result.get(0).getTimes(2), dTime);
        assertArrayEquals(result.get(0).getTimes(3), fTime);

    }
}
