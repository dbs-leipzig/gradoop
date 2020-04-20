package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.junit.Assert.assertEquals;

public class AddEmbeddingsTPGMElementsTest extends PhysicalTPGMOperatorTest {

    @Test
    public void testAddEmbeddings() throws Exception {
        EmbeddingTPGM e1 = createEmbeddingTPGM(GradoopId.get(), GradoopId.get());
        EmbeddingTPGM e2 = createEmbeddingTPGM(GradoopId.get(), GradoopId.get(), GradoopId.get());

        DataSet<EmbeddingTPGM> input = ExecutionEnvironment.getExecutionEnvironment().fromElements(e1, e2);

        DataSet<EmbeddingTPGM> result = new AddEmbeddingsTPGMElements(input, 3).evaluate();

        assertEquals(2, result.count());
        assertEquals(result.collect().get(0).size(), 5);
        assertEquals(result.collect().get(1).size(), 6);
    }
}
