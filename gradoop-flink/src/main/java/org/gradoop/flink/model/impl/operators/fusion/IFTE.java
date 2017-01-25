package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.functions.MapFunction;

import org.gradoop.flink.model.api.functions.Function;

/**
 * Created by vasistas on 24/01/17.
 */
public class IFTE<K,V> implements MapFunction<Boolean,Function<K,V>> {

    private final Function<K,V> isTrue, isFalse;

    public IFTE(Function<K, V> isTrue, Function<K, V> isFalse) {
        this.isTrue = isTrue;
        this.isFalse = isFalse;
    }

    @Override
    public Function<K, V> map(Boolean aBoolean) throws Exception {
        Function<K,V> f = k -> (aBoolean ? isTrue.apply(k) : isFalse.apply(k));
        return f;
    }
}
