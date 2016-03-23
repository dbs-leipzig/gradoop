package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.datagen.foodbroker.generator.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Product<V extends EPGMVertex> extends
  RichMapFunction<MasterDataSeed, MasterDataObject<V>>
  implements ResultTypeQueryable<MasterDataObject<V>> {

  private final EPGMVertexFactory<V> vertexFactory;

  private List<Tuple2<String, String>> nameGroupPairs;
  private List<String> adjectives;

  private Integer nameGroupPairCount;
  private Integer adjectiveCount;

  public Product(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    nameGroupPairs = getRuntimeContext()
      .getBroadcastVariable(ProductGenerator.NAMES_GROUPS_BC);
    adjectives = getRuntimeContext()
      .getBroadcastVariable(ProductGenerator.ADJECTIVES_BC);

    nameGroupPairCount = nameGroupPairs.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public MasterDataObject<V> map(MasterDataSeed seed) throws  Exception {

    Random random = new Random();

    Tuple2<String, String> nameGroupPair = nameGroupPairs
      .get(random.nextInt(nameGroupPairCount));

    String bid = "PRD" + seed.getLongId().toString();
    String name = adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nameGroupPair.f0;
    String category = nameGroupPair.f1;

    PropertyList properties = new PropertyList();

    properties.set("name", name);
    properties.set("category", category);

    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);

    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    V vertex = vertexFactory
      .createVertex(ProductGenerator.CLASS_NAME, properties);

    return new MasterDataObject<>(seed, vertex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<MasterDataObject<V>> getProducedType() {
    return new TupleTypeInfo<>(
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.SHORT_TYPE_INFO,
      TypeExtractor.createTypeInfo(vertexFactory.getType()));
  }
}
