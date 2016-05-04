package org.gradoop.model.impl.operators.matching.common.debug;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;
import java.util.Map;

/**
 * Base class for printing debug output using vertex and edge mappings. Both
 * map a {@link GradoopId} to a readable numeric id.
 */
@FunctionAnnotation.ForwardedFields("*")
public abstract class Printer<IN> extends RichMapFunction<IN, IN> {

  public static final String VERTEX_MAPPING = "vertexMapping";

  public static final String EDGE_MAPPING = "edgeMapping";

  protected Map<GradoopId, Integer> vertexMap;

  protected Map<GradoopId, Integer> edgeMap;

  protected final String prefix;

  protected final boolean isIterative;

  public Printer() {
    this(false, "");
  }

  public Printer(boolean isIterative, String prefix) {
    this.isIterative  = isIterative;
    this.prefix       = prefix;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Tuple2<GradoopId, Integer>> vertexMapping = getRuntimeContext()
      .getBroadcastVariable(VERTEX_MAPPING);
    vertexMap = initMapping(vertexMapping);
    List<Tuple2<GradoopId, Integer>> edgeMapping = getRuntimeContext()
      .getBroadcastVariable(EDGE_MAPPING);
    edgeMap = initMapping(edgeMapping);
  }

  @Override
  public IN map(IN in) throws Exception {
    System.out.println(String.format("%s%s",getHeader(), getDebugString(in)));
    return in;
  }

  protected abstract String getDebugString(IN in);

  protected String getHeader() {
    return String.format("[%d][%s]: ",
      isIterative ? getIterationRuntimeContext().getSuperstepNumber() : 0,
      prefix != null && !prefix.isEmpty() ? prefix : " ");
  }

  private Map<GradoopId, Integer> initMapping(List<Tuple2<GradoopId, Integer>> tuples) {
    Map<GradoopId, Integer> map = Maps.newHashMap();
    for (Tuple2<GradoopId, Integer> tuple : tuples) {
      map.put(tuple.f0, tuple.f1);
    }
    return map;
  }

  protected List<Integer> convertList(List<GradoopId> gradoopIds, boolean isVertex) {
    List<Integer> result = Lists.newArrayListWithCapacity(gradoopIds.size());
    for (GradoopId gradoopId : gradoopIds) {
      result.add(isVertex ? vertexMap.get(gradoopId) : edgeMap.get(gradoopId));
    }
    return result;
  }
}
