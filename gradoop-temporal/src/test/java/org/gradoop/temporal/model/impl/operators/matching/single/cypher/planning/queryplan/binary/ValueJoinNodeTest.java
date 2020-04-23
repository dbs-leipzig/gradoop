package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ValueJoinNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.MockPlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.gradoop.common.GradoopTestUtils.call;

import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEmbeddingTPGM;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ValueJoinNodeTest {
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

        ValueJoinNode node = new ValueJoinNode(leftMockNode, rightMockNode,
                singletonList(Pair.of("v1", "age")), singletonList(Pair.of("v3", "age")),
                new ArrayList<>(), new ArrayList<>(),
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
        assertThat(outputMetaData.getTimeColumn("e1"), is(1));
        assertThat(outputMetaData.getTimeColumn("v3"), is(2));
        assertThat(outputMetaData.getTimeColumn("e2"), is(3));
        assertThat(outputMetaData.getTimeColumn("v4"), is(4));
    }

    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testGetJoinProperties() throws Exception {
        EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
        leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        leftInputMetaData.setPropertyColumn("v1", "age", 0);
        leftInputMetaData.setPropertyColumn("e1", "since", 1);
        leftInputMetaData.setTimeColumn("v1",0);
        leftInputMetaData.setTimeColumn("e1", 1);
        leftInputMetaData.setTimeColumn("v2", 2);

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

        PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
        PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

        ValueJoinNode node = new ValueJoinNode(leftChild, rightChild,
                singletonList(Pair.of("v1", "age")), singletonList(Pair.of("v4", "age")),
                new ArrayList<>(), new ArrayList<>(),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        assertThat(call(ValueJoinNode.class, node, "getJoinPropertiesLeft"), is(asList(0)));
        assertThat(call(ValueJoinNode.class, node, "getJoinPropertiesRight"), is(asList(2)));
    }

    @Test
    public void testGetDistinctColumnsIsomorphism() throws Exception {
        EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
        leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        leftInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        leftInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        leftInputMetaData.setEntryColumn("e2", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
        leftInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
        leftInputMetaData.setTimeColumn("v1",0);
        leftInputMetaData.setTimeColumn("e1", 1);
        leftInputMetaData.setTimeColumn("v2", 2);
        leftInputMetaData.setTimeColumn("e2",3);
        leftInputMetaData.setTimeColumn("v3", 4);

        EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
        rightInputMetaData.setEntryColumn("v4", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e3", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v5", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setEntryColumn("e4", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
        rightInputMetaData.setEntryColumn("v6", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
        rightInputMetaData.setTimeColumn("v4", 0);
        rightInputMetaData.setTimeColumn("e3", 1);
        rightInputMetaData.setTimeColumn("v5", 2);
        rightInputMetaData.setTimeColumn("e4", 3);
        rightInputMetaData.setTimeColumn("v6", 4);

        PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
        PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

        ValueJoinNode node = new ValueJoinNode(leftChild, rightChild,
                Lists.newArrayList(), Lists.newArrayList(),
                new ArrayList<>(), new ArrayList<>(),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        assertThat(call(ValueJoinNode.class, node, "getDistinctVertexColumnsLeft"), is(asList(0, 2, 4)));
        assertThat(call(ValueJoinNode.class, node, "getDistinctVertexColumnsRight"), is(asList(0, 2, 4)));
        assertThat(call(ValueJoinNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList(1, 3)));
        assertThat(call(ValueJoinNode.class, node, "getDistinctEdgeColumnsRight"), is(asList(1, 3)));
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
        leftInputMetaData.setTimeColumn("v1",0);
        leftInputMetaData.setTimeColumn("e1", 1);
        leftInputMetaData.setTimeColumn("v2", 2);
        leftInputMetaData.setTimeColumn("e2",3);
        leftInputMetaData.setTimeColumn("v3", 4);

        EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
        rightInputMetaData.setEntryColumn("v4", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e3", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v5", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setEntryColumn("e4", EmbeddingTPGMMetaData.EntryType.EDGE, 3);
        rightInputMetaData.setEntryColumn("v6", EmbeddingTPGMMetaData.EntryType.VERTEX, 4);
        rightInputMetaData.setTimeColumn("v4", 0);
        rightInputMetaData.setTimeColumn("e3", 1);
        rightInputMetaData.setTimeColumn("v5", 2);
        rightInputMetaData.setTimeColumn("e4", 3);
        rightInputMetaData.setTimeColumn("v6", 4);

        PlanNode leftChild = new MockPlanNode(null, leftInputMetaData);
        PlanNode rightChild = new MockPlanNode(null, rightInputMetaData);

        ValueJoinNode node = new ValueJoinNode(leftChild, rightChild,
                Lists.newArrayList(), Lists.newArrayList(),
                new ArrayList<>(), new ArrayList<>(),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

        assertThat(call(ValueJoinNode.class, node, "getDistinctVertexColumnsLeft"), is(asList()));
        assertThat(call(ValueJoinNode.class, node, "getDistinctVertexColumnsRight"), is(asList()));
        assertThat(call(ValueJoinNode.class, node, "getDistinctEdgeColumnsLeft"), is(asList()));
        assertThat(call(ValueJoinNode.class, node, "getDistinctEdgeColumnsRight"), is(asList()));
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

        Long[] aTime = new Long[]{1L, 2L, 3L, 4L};
        Long[] bTime = new Long[]{11L, 12L, 13L, 14L};
        Long[] cTime = new Long[]{21L, 22L, 23L, 24L};
        Long[] dTime = new Long[]{31L, 32L, 33L, 34L};
        Long[] eTime = new Long[]{1L, 42L, 43L, 44L};
        Long[] fTime = new Long[]{51L, 52L, 53L, 54L};
        Long[] gTime = new Long[]{61L, 62L, 63L, 64L};
        Long[] hTime = new Long[]{71L, 72L, 73L, 74L};

        EmbeddingTPGMMetaData leftInputMetaData = new EmbeddingTPGMMetaData();
        leftInputMetaData.setEntryColumn("v1", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        leftInputMetaData.setPropertyColumn("v1", "age", 0);
        leftInputMetaData.setTimeColumn("v1", 0);

        EmbeddingTPGM embedding1 = createEmbeddingTPGM(new GradoopId[]{a}, new Long[][]{aTime});
        embedding1.addPropertyValues(PropertyValue.create(42));

        EmbeddingTPGM embedding2 = createEmbeddingTPGM(new GradoopId[]{b}, new Long[][]{bTime});
        embedding2.addPropertyValues(PropertyValue.create(21));

        DataSet<EmbeddingTPGM> leftEmbeddings = env.fromElements(embedding1, embedding2);


         /* ------------------------------------------------------------------
         * |  v2   | e1    | v3    | v3.age |  v2Time  |  e1Time  |  v3Time  |
         * ------------------------------------------------------------------
         * | id(c) | id(d) | id(e) |  42    |  cTime   |  dTime   |  eTime   | -> Embedding 3
         * -------------------------------------------------------------------
         * | id(f) | id(g) | id(h) |  21    |  fTime   |  gTime   |  hTime   | -> Embedding 4
         * -------------------------------------------------------------------
          */

        EmbeddingTPGMMetaData rightInputMetaData = new EmbeddingTPGMMetaData();
        rightInputMetaData.setEntryColumn("v2", EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        rightInputMetaData.setEntryColumn("e1", EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        rightInputMetaData.setEntryColumn("v3", EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        rightInputMetaData.setPropertyColumn("v3", "age", 0);
        rightInputMetaData.setTimeColumn("v2", 0);
        rightInputMetaData.setTimeColumn("e1", 1);
        rightInputMetaData.setTimeColumn("v3", 2);

        EmbeddingTPGM embedding3 = createEmbeddingTPGM(new GradoopId[]{c,d,e},
                new Long[][]{cTime, dTime, eTime});
        embedding3.addPropertyValues(PropertyValue.create(42));

        EmbeddingTPGM embedding4 = createEmbeddingTPGM(new GradoopId[]{f,g,h},
                new Long[][]{fTime, gTime, hTime});
        embedding4.addPropertyValues(PropertyValue.create(21));

        DataSet<EmbeddingTPGM> rightEmbeddings = env.fromElements(embedding3, embedding4);

        MockPlanNode leftChild = new MockPlanNode(leftEmbeddings, leftInputMetaData);
        MockPlanNode rightChild = new MockPlanNode(rightEmbeddings, rightInputMetaData);

        ValueJoinNode node = new ValueJoinNode(leftChild, rightChild,
                singletonList(Pair.of("v1", "age")), singletonList(Pair.of("v3", "age")),
                new ArrayList<>(), new ArrayList<>(),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        List<EmbeddingTPGM> result = node.execute().collect();
        result.sort(Comparator.comparing(o -> o.getProperty(0))); // sort by property value in column 0

        assertThat(result.size(), is(2));

        assertEmbeddingTPGM(result.get(0), asList(b, f, g, h),
                asList(PropertyValue.create(21), PropertyValue.create(21)),
                asList(bTime, fTime, gTime, hTime));
        assertEmbeddingTPGM(result.get(1), asList(a, c, d, e),
                asList(PropertyValue.create(42), PropertyValue.create(42)),
                asList(aTime, cTime, dTime, eTime));

        // now with temporal join
        node = new ValueJoinNode(leftChild, rightChild,
                singletonList(Pair.of("v1", "age")), singletonList(Pair.of("v3", "age")),
                singletonList(Pair.of("v1", "tx_from")), singletonList(Pair.of("v3", "tx_from")),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        result = node.execute().collect();
        assertEquals(result.size(), 1);
        assertEmbeddingTPGM(result.get(0), asList(a, c, d, e),
                asList(PropertyValue.create(42), PropertyValue.create(42)),
                asList(aTime, cTime, dTime, eTime));
    }
}
