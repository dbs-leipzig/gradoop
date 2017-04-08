package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Created by vasistas on 08/04/17.
 */
public class StringAsEdge implements FlatMapFunction<String, ImportEdge<String>> {

  private final ImportEdge<String> reusable;

  public StringAsEdge() {
    reusable = new ImportEdge<>();
    reusable.setProperties(new Properties());
  }

  @Override
  public void flatMap(String value, Collector<ImportEdge<String>> out) throws Exception {
    String[] arguments = value.split(" ");
    if (arguments.length == 3) {
      reusable.setId(value);
      reusable.setSourceId(arguments[0]);
      reusable.setLabel(arguments[1]);
      reusable.setTargetId(arguments[2]);
      out.collect(reusable);
    }
  }
}
