package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.datagen.foodbroker.generator.MasterDataGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator
  .AbstractMasterDataGenerator;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;

import java.util.Objects;

/**
 * Created by peet on 24.03.16.
 */
public class GoodOrBad<V extends EPGMVertex> implements
  FlatMapFunction<MasterDataObject<V>, Tuple2<Long, Short>> {

  @Override
  public void flatMap(MasterDataObject<V> masterDataObject,
    Collector<Tuple2<Long, Short>> collector) throws Exception {

    if(!Objects.equals(masterDataObject.getQuality(),
      AbstractMasterDataGenerator.NORMAL_VALUE)) {

      collector.collect(
        new Tuple2<>(masterDataObject.getId(), masterDataObject.getQuality()));
    }
  }
}
