package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

public class TSVToEdge <E extends EPGMEdge>
  implements MapFunction<String, E> {


  private final EPGMEdgeFactory<E> edgeFactory;

  public TSVToEdge(EPGMEdgeFactory<E> edgeFactory){
    this.edgeFactory=edgeFactory;
  }


  @Override
  public E map(String s) throws Exception {
    GradoopId edgeID = GradoopId.get();
    String edgeLabel = "";
    GradoopId sourceID = GradoopId.get();
    GradoopId targetID = GradoopId.get();
    PropertyList properties = PropertyList.create();
    GradoopIdSet graphs = GradoopIdSet.fromExisting(GradoopId.get());


    return  edgeFactory.initEdge(edgeID, edgeLabel, sourceID, targetID,
      properties, graphs);
  }
}
