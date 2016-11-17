package org.gradoop.flink.util;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Collection;


public class Collect<T> implements GroupReduceFunction<T, Collection<T>> {

  @Override
  public void reduce(Iterable<T> iterable, Collector<Collection<T>> collector) throws Exception {
    Collection<T> collection = Lists.newArrayList();

    for (T item : iterable) {
      collection.add(item);
    }

    collector.collect(collection);
  }
}
