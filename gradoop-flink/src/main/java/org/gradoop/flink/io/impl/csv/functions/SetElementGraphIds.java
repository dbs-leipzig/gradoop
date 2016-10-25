package org.gradoop.flink.io.impl.csv.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class SetElementGraphIds<T extends EPGMGraphElement>
  extends RichGroupReduceFunction<Tuple2<T, String>, T> {

  public static final String BROADCAST_GRAPHHEADS = "graphHeads";

  private List<GraphHead> graphHeads;

  private Map<String, GradoopId> keyIdMap;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphHeads = getRuntimeContext().getBroadcastVariable(BROADCAST_GRAPHHEADS);
    keyIdMap = Maps.newHashMapWithExpectedSize(graphHeads.size());
    for (GraphHead graph : graphHeads) {
      keyIdMap.put(
        graph.getPropertyValue(CSVToElement.PROPERTY_KEY_KEY).getString(),
        graph.getId());
    }
  }

  @Override
  public void reduce(Iterable<Tuple2<T, String>> iterable,
    Collector<T> collector) throws Exception {
    Iterator<Tuple2<T, String>> iterator = iterable.iterator();
    Tuple2<T, String> tuple = iterator.next();
    T element = tuple.f0;
    element.addGraphId(keyIdMap.get(tuple.f1));
    while (iterator.hasNext()) {
      tuple = iterator.next();
      element.addGraphId(keyIdMap.get(tuple.f1));
    }
    collector.collect(element);
  }

}
