package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.TemporalEdgeWithTiePoint;

/**
 * Extract the GradoopID of the start vertex of a TemporalEdgeWithTiePoint
 */
public class ExtractEdgeStartID implements KeySelector<TemporalEdgeWithTiePoint, GradoopId> {

    @Override
    public GradoopId getKey(TemporalEdgeWithTiePoint value) throws Exception {
        return value.getSource();
    }
}
