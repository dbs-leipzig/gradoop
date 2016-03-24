package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;

import java.util.HashMap;
import java.util.Random;

public class RandomId<V extends EPGMVertex>
  implements MapFunction<TransactionalDataObject<V>,
  TransactionalDataObject<V>>, ResultTypeQueryable<TransactionalDataObject<V>> {

  private final Integer count;
  private final EPGMVertexFactory<V> vertexFactory;

  public RandomId(Integer count, EPGMVertexFactory<V> vertexFactory) {
    this.count = count;
    this.vertexFactory = vertexFactory;
  }

  @Override
  public TransactionalDataObject<V> map(
    TransactionalDataObject<V> transactionalDataObject) throws Exception {

    transactionalDataObject.setJoinKey((long) new Random().nextInt(count));

    return transactionalDataObject;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<TransactionalDataObject<V>> getProducedType() {
    return new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO,
      TypeExtractor.createTypeInfo(HashMap.class),
      TypeExtractor.createTypeInfo(vertexFactory.getType()));
  }
}
