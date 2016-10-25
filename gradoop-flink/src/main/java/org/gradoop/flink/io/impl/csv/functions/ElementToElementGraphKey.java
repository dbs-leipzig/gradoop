package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by Stephan on 21.10.16.
 */
public class ElementToElementGraphKey<T extends EPGMGraphElement> implements
  FlatMapFunction<T, Tuple2<T, String>> {

  @Override
  public void flatMap(T element, Collector<Tuple2<T, String>> collector)
    throws Exception {
    String graphs = element.getPropertyValue(CSVToElement.PROPERTY_KEY_GRAPHS).getString();
    for (String graph : graphs.split(CSVToElement.SEPARATOR_GRAPHS)) {
      graph.replaceAll(CSVToElement.ESCAPE_REPLACEMENT_GRAPHS, CSVToElement.SEPARATOR_GRAPHS);
      collector.collect(new Tuple2<>(element, graph));
    }
  }
}
