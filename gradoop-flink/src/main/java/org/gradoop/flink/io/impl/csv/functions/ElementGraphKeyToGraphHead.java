package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by Stephan on 21.10.16.
 */
public class ElementGraphKeyToGraphHead<T extends EPGMGraphElement> implements
  GroupReduceFunction<Tuple2<T, String>, GraphHead> {

  private GraphHeadFactory graphHeadFactory;

  public ElementGraphKeyToGraphHead(GraphHeadFactory graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public void reduce(Iterable<Tuple2<T, String>> iterable,
    Collector<GraphHead> collector) throws Exception {
    GraphHead graphHead = graphHeadFactory.createGraphHead();
    graphHead.setProperty(CSVToElement.PROPERTY_KEY_KEY,
      iterable.iterator().next().f1);
    collector.collect(graphHead);
  }
}
