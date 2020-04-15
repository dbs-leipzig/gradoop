package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang.ArrayUtils;
import org.gradoop.common.model.impl.id.GradoopId;

import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;



/**
 * Practically identical to ExtractJoinColumnsTest in flink, but adjusted to temporal data
 */
public class ExtractJoinColumnsTest {
    @Test
    public void testSingleColumn() throws Exception {
        GradoopId v0 = GradoopId.get();
        GradoopId v1 = GradoopId.get();

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(v0);
        embedding.add(v1);

        ExtractJoinColumns udf = new ExtractJoinColumns(Collections.singletonList(0));

        Assert.assertEquals(ArrayUtils.toString(v0.toByteArray()), udf.getKey(embedding));
    }

    @Test
    public void testMultiColumn() throws Exception {
        GradoopId v0 = GradoopId.get();
        GradoopId v1 = GradoopId.get();

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(v0);
        embedding.add(v1);

        ExtractJoinColumns udf = new ExtractJoinColumns(Arrays.asList(0, 1));

        Assert.assertEquals(
                ArrayUtils.toString(v0.toByteArray()) + ArrayUtils.toString(v1.toByteArray()),
                udf.getKey(embedding)
        );
    }

    @Test
    public void testMultiColumnReverse() throws Exception {
        GradoopId v0 = GradoopId.get();
        GradoopId v1 = GradoopId.get();

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(v0);
        embedding.add(v1);

        ExtractJoinColumns udf1 = new ExtractJoinColumns(Arrays.asList(0, 1));
        ExtractJoinColumns udf2 = new ExtractJoinColumns(Arrays.asList(1, 0));

        Assert.assertNotEquals(udf1.getKey(embedding), udf2.getKey(embedding));
    }
}
