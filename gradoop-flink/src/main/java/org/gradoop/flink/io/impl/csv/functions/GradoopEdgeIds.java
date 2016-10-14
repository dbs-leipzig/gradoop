package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.util.HashMap;
import java.util.Map;


public class GradoopEdgeIds extends RichMapFunction<Edge, Edge> {

  public static final String ID_MAP = "idMap";

  private Map<String, GradoopId> map;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    map = getRuntimeContext()
      .<HashMap<String, GradoopId>>getBroadcastVariable(ID_MAP).get(0);
  }

  @Override
  public Edge map(Edge edge) throws Exception {
    PropertyList properties = edge.getProperties();
    edge.setSourceId(map.get(properties.get("source").getString()));
    edge.setTargetId(map.get(properties.get("target").getString()));
    return edge;
  }
}
