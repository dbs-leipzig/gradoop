package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTPGMEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import org.apache.flink.api.java.DataSet;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.*;
import static org.junit.Assert.assertEquals;

public class JoinTPGMEmbeddingsTest {
    private static GradoopId v0 = GradoopId.get();
    private static GradoopId v1 = GradoopId.get();
    private static GradoopId e0 = GradoopId.get();
    private static GradoopId e1 = GradoopId.get();
    private static GradoopId e2 = GradoopId.get();
    private static GradoopId e3 = GradoopId.get();

    @Test
    public void testJoin() throws Exception {
        ExecutionEnvironment env = getExecutionEnvironment();
        EmbeddingTPGM l = new EmbeddingTPGM();
        l.add(v0, new PropertyValue[]{PropertyValue.create("Foobar")},
                1L, 2L, 3L, 4L);
        l.add(e0, new PropertyValue[]{PropertyValue.create(42)},
                5L, 6L, 7L, 8L);
        l.add(v1, new PropertyValue[]{}, 9L, 10L, 11L, 12L);
        DataSet<EmbeddingTPGM> left = env.fromElements(l);

        EmbeddingTPGM r = new EmbeddingTPGM();
        r.add(v1, new PropertyValue[]{PropertyValue.create("Baz")},
                1L, 2L, 3L, 4L);
        DataSet<EmbeddingTPGM> right = env.fromElements(r);

        DataSet<EmbeddingTPGM> result =
                new JoinTPGMEmbeddings(left, right, 1, 2, 0)
                        .evaluate();
        assertEquals(1, result.count());
        assertEveryEmbeddingTPGM(result, embedding ->
                embedding.getProperties().equals(Lists.newArrayList(
                        PropertyValue.create("Foobar"),
                        PropertyValue.create(42),
                        PropertyValue.create("Baz")
                        )
                ));
        assertEveryEmbeddingTPGM(result, embedding ->
                embedding.getTimes(0).equals(new Long[]{1L, 2L, 3L, 4L}));
        assertEveryEmbeddingTPGM(result, embedding ->
                embedding.getTimes(1).equals(new Long[]{5L, 6L, 7L, 8L}));
        assertEveryEmbeddingTPGM(result, embedding ->
                embedding.getTimes(2).equals(new Long[]{9L, 10L, 11L, 12L}));
        assertEveryEmbeddingTPGM(result, embedding ->
                embedding.getTimes(3).equals(new Long[]{1L, 2L, 3L, 4L}));
        assertEmbeddingTPGMExists(result, v0, e0, v1);
    }

    @Test
    public void testSingleJoinPartners() throws Exception {
        ExecutionEnvironment env = getExecutionEnvironment();

        DataSet<EmbeddingTPGM> left = env.fromElements(
                createEmbeddingTPGM(v0, e0, v1),
                createEmbeddingTPGM(v1, e2, v0)
        );

        DataSet<EmbeddingTPGM> right = env.fromElements(
                createEmbeddingTPGM(v0),
                createEmbeddingTPGM(v1)
        );

        PhysicalTPGMOperator join = new JoinTPGMEmbeddings(left, right, 1, 0, 0);

        DataSet<EmbeddingTPGM> result = join.evaluate();
        assertEquals(2, result.count());
        assertEmbeddingTPGMExists(result, v0, e0, v1);
        assertEmbeddingTPGMExists(result, v1, e2, v0);
    }

    @Test
    public void testMultipleJoinPartners() throws Exception {
        ExecutionEnvironment env = getExecutionEnvironment();
        //Single Column
        DataSet<EmbeddingTPGM> left = env.fromElements(
                createEmbeddingTPGM(v0, e0, v1),
                createEmbeddingTPGM(v1, e1, v0)
        );

        DataSet<EmbeddingTPGM> right = env.fromElements(
                createEmbeddingTPGM(v0, e2, v1),
                createEmbeddingTPGM(v1, e3, v0)
        );

        PhysicalTPGMOperator join =
                new JoinTPGMEmbeddings(left, right, 3, Lists.newArrayList(0, 2), Lists.newArrayList(2, 0));

        DataSet<EmbeddingTPGM> result = join.evaluate();
        assertEquals(2, result.count());
        assertEmbeddingTPGMExists(result, v0, e0, v1, e3);
        assertEmbeddingTPGMExists(result, v1, e1, v0, e2);
    }
}
