package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.api.entities.EPGMElement;

/**
 * Created by Giacomo Bergami on 25/01/17.
 */
/**
 * EPGMElement with properties => properties
 *
 * @param <L> EPGMElement type having properties
 */
@FunctionAnnotation.ForwardedFields("properties->*")
public class TmpFusion<L extends EPGMElement>
        implements MapFunction<L, org.gradoop.common.model.impl.properties.Properties>,
        KeySelector<L, org.gradoop.common.model.impl.properties.Properties> {

    @Override
    public org.gradoop.common.model.impl.properties.Properties map(L l) throws Exception {
        return l.getProperties();
    }

    @Override
    public org.gradoop.common.model.impl.properties.Properties getKey(L l) throws Exception {
        return l.getProperties();
    }
}

