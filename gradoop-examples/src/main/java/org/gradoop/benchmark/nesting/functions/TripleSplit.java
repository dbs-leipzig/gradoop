package org.gradoop.benchmark.nesting.functions;

import org.gradoop.benchmark.nesting.parsers.ConvertStringToEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Parsing an edge, skipping empty strings
 */
public class TripleSplit implements ConvertStringToEdge<String,String> {

  /**
   * Reusable element
   */
  private final ImportEdge<String> reusable;

  public TripleSplit() {
    this.reusable = new ImportEdge<>();
  }

  @Override
  public boolean isValid(String toCheck) {
    return toCheck.length()>0;
  }

  @Override
  public ImportEdge<String> map(String s) throws Exception {
    String array[] = s.split(" ");
    reusable.setLabel(array[1]);
    reusable.setSourceId(array[0]);
    reusable.setTargetId(array[2]);
    reusable.setId(s);
    return reusable;
  }
}
