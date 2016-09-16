package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.io.impl.csv.pojos.Csv;


public class XmlToGraphhead implements MapFunction<Csv, GraphHead> {

  private ExecutionEnvironment env;

  public XmlToGraphhead(ExecutionEnvironment env) {
    this.env = env;
  }

  @Override
  public GraphHead map(Csv s) throws Exception {
    env.readCsvFile(s.getName());

    return null;
  }
}
