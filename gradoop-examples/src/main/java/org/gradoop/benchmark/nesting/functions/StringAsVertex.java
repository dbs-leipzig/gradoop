package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFields("* -> f0")
public class StringAsVertex implements MapFunction<String, ImportVertex<String>> {

  private final ImportVertex<String> reusable;

  public StringAsVertex() {
    reusable = new ImportVertex<>();
    reusable.setProperties(new Properties());
    reusable.setLabel("");
  }

  @Override
  public ImportVertex<String> map(String value) throws Exception {
    reusable.setId(value);
    return reusable;
  }
}
