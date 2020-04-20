package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions.util;

import java.io.Serializable;
import java.util.function.Function;

/**
 * extends Functional interface to make a function serializable so it can be used in Flink.
 */
public interface SerializableFunction<T,R> extends Function<T,R>, Serializable {
}
