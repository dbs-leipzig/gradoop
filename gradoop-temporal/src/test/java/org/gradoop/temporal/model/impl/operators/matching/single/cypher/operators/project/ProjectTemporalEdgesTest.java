package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.ProjectTemporalEdges;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;


import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEveryEmbeddingTPGM;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProjectTemporalEdgesTest extends PhysicalTPGMOperatorTest {
    @Test
    public void returnsEmbeddingTPGMWithIdProjectionId() throws Exception {
        ArrayList<String> propertyNames = Lists.newArrayList("foo", "bar", "baz");
        TemporalEdgeFactory factory = new TemporalEdgeFactory();
        TemporalEdge e1 = factory.initEdge(GradoopId.get(), "label",
                GradoopId.get(), GradoopId.get(), getProperties(propertyNames));
        e1.setTransactionTime(new Tuple2<>(1L, 2L));
        e1.setValidTime(new Tuple2<>(3L, 4L));
        TemporalEdge e2 = factory.initEdge(GradoopId.get(), "label",
                GradoopId.get(), GradoopId.get(), getProperties(propertyNames));
        e2.setTransactionTime(new Tuple2<>(5L, 6L));
        e2.setValidTime(new Tuple2<>(7L, 8L));
        DataSet<TemporalEdge> edgeDataSet = getExecutionEnvironment().fromElements(e1, e2);
        ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");

        ProjectTemporalEdges operator = new ProjectTemporalEdges(edgeDataSet, extractedPropertyKeys, false);
        DataSet<EmbeddingTPGM> results = operator.evaluate();

        assertEquals(2, results.count());
        assertEveryEmbeddingTPGM(results, (embedding) -> {
            assertEquals(3, embedding.size());
            assertEquals(PropertyValue.create("foo"), embedding.getProperty(0));
            assertEquals(PropertyValue.create("bar"), embedding.getProperty(1));
        });
        assertArrayEquals(results.collect().get(0).getTimes(0), new Long[]{1L, 2L, 3L, 4L});
        assertArrayEquals(results.collect().get(1).getTimes(0), new Long[]{5L, 6L, 7L, 8L});
    }

    @Test
    public void testProjectLoop() throws Exception {
        GradoopId a = GradoopId.get();
        TemporalEdge edge = new TemporalEdgeFactory().createEdge(a, a);
        edge.setTransactionTime(new Tuple2<>(5L, 6L));
        edge.setValidTime(new Tuple2<>(7L, 8L));

        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(edge);

        EmbeddingTPGM result = new ProjectTemporalEdges(edges, Collections.emptyList(), true)
                .evaluate().collect().get(0);

        assertEquals(result.size(), 2);
        assertEquals(a, result.getId(0));
        assertEquals(edge.getId(), result.getId(1));
        assertArrayEquals(result.getTimes(0), new Long[]{5L, 6L, 7L, 8L});
    }
}
