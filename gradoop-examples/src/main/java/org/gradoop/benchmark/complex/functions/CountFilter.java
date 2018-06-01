package org.gradoop.benchmark.complex.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class CountFilter implements FilterFunction<Edge> {
    @Override
    public boolean filter(Edge edge) {
        PropertyValue value = edge.getPropertyValue("count");
        return value.getLong() > 500;
    }
}
