package org.gradoop.flink.model.impl.functions.epgm.filters;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.functions.epgm.IdInBroadcast;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AbstractRichCombinedFilterFunctionTest
        extends GradoopFlinkTestBase {

    /**
     * A test {@link RichFilterFunction} providing a method to check if
     * {@link RichFilterFunction#open(Configuration)} and
     * {@link RichFilterFunction#close()} were called.
     */
    private static class TestRichCombinableFilter
            extends RichFilterFunction<Integer> {
        private boolean wasOpenCalled = false;
        private boolean wasCloseCalled = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            assertNotNull("Parameters not set", parameters);
            assertNotNull("RuntimeContext not set", getRuntimeContext());
            wasOpenCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            wasCloseCalled = true;
        }

        @Override
        public boolean filter(Integer value) throws Exception {
            if (wasOpenCalled) {
                //
            }
            if (wasCloseCalled) {
                //
            }
            assertNotNull("Context not set.", getRuntimeContext());
            return true;
        }

        public void validate() {
            assertTrue("open(Configuration) was not called", wasOpenCalled);
            assertTrue("close() was not called", wasCloseCalled);
        }
    }

    @Test
    public void testIfContextIsSet() throws Exception {
        DataSet<Integer> elements =
                getExecutionEnvironment().fromElements(1, 2, 3, 4, 5);
        TestRichCombinableFilter filter1 = new TestRichCombinableFilter();
        TestRichCombinableFilter filter2 = new TestRichCombinableFilter();
        Collection<Integer> collection = new ArrayList<>();
        elements.filter(new And<Integer>(filter1, filter2))
                .output(new LocalCollectionOutputFormat<>(collection));
        getExecutionEnvironment().execute();
        filter1.validate();
        filter2.validate();
    }

    @Test
    public void testAnd() throws Exception {
        VertexFactory factory = getConfig().getVertexFactory();
        Vertex vertex1 = factory.createVertex();
        Vertex vertex2 = factory.createVertex();
        GradoopIdSet both = GradoopIdSet.fromExisting(vertex1.getId(),
                vertex2.getId());
        List<Vertex> collect = Stream.generate(factory::createVertex).limit(100)
                .collect(Collectors.toCollection(ArrayList::new));
        collect.add(vertex1);
        collect.add(vertex2);
        getExecutionEnvironment().fromCollection(collect)
                .filter(new And<>(new IdInBroadcast<>(), new IdInBroadcast<>()))
                .withBroadcastSet(getExecutionEnvironment()
                        .fromElements(vertex1.getId(), vertex2.getId()),
                        IdInBroadcast.IDS).print();
    }
}
