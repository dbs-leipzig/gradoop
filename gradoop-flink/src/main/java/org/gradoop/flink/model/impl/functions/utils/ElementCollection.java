package org.gradoop.flink.model.impl.functions.utils;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Collection;

public class ElementCollection<T>
  implements GroupReduceFunction<T, Collection<T>> {


  @Override
  public void reduce(Iterable<T> values, Collector<Collection<T>> out) throws
    Exception {

    out.collect(Lists.newArrayList(values));
  }
}
