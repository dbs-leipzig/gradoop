package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;

public class MasterDataVertex<V extends EPGMVertex>
  implements MapFunction<MasterDataObject, V>, ResultTypeQueryable<V> {

  private final EPGMVertexFactory<V> vertexFactory;

  public MasterDataVertex(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public V map(MasterDataObject masterDataObject) throws Exception {
    return vertexFactory.createVertex(
      masterDataObject.getId(),
      masterDataObject.getLabel(),
      masterDataObject.getProperties()
    );
  }

  @Override
  public TypeInformation<V> getProducedType() {
    return TypeExtractor.createTypeInfo(vertexFactory.getType());
  }
}
