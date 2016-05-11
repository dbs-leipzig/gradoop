package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractMasterDataGenerator<V extends EPGMVertex>
  implements MasterDataGenerator<V> {

  static final Integer GOOD_VALUE = 1;
  static final Integer NORMAL_VALUE = 0;
  static final Integer BAD_VALUE = -1;

  final FoodBrokerConfig foodBrokerConfig;
  final ExecutionEnvironment env ;
  final EPGMVertexFactory<V> vertexFactory;


  AbstractMasterDataGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    this.foodBrokerConfig = foodBrokerConfig;
    this.env = gradoopFlinkConfig.getExecutionEnvironment();
    this.vertexFactory = gradoopFlinkConfig.getVertexFactory();
  }

  List<MasterDataSeed> getMasterDataSeeds(String className) {
    Double goodRatio = foodBrokerConfig.getMasterDataGoodRatio(className);

    Double badRatio = foodBrokerConfig.getMasterDataBadRatio(className);

    Integer count = foodBrokerConfig.getMasterDataCount(className);

    Integer goodCount = (int) Math.round(count * goodRatio);
    Integer badCount = (int) Math.round(count * badRatio);
    Integer normalCount = count - goodCount - badCount;

    List<MasterDataSeed> seedList = new ArrayList<>();

    Map<Integer, Integer> qualityCounts = new HashMap<>();

    qualityCounts.put(AbstractMasterDataGenerator.GOOD_VALUE, goodCount);
    qualityCounts.put(AbstractMasterDataGenerator.NORMAL_VALUE, normalCount);
    qualityCounts.put(AbstractMasterDataGenerator.BAD_VALUE, badCount);

    for(Map.Entry<Integer, Integer> qualityCount : qualityCounts.entrySet()) {

      Integer quality = qualityCount.getKey();

      for(int i = 1; i <= qualityCount.getValue(); i++) {
        seedList.add(new MasterDataSeed(quality));
      }
    }

    Collections.shuffle(seedList);

    return seedList;
  }

  List<String> getStringValuesFromFile(String fileName) {
    fileName = "/foodbroker/" + fileName;


    List<String> values = null;

    try {
      String adjectivesPath = EmployeeGenerator.class
        .getResource(fileName).getFile();

      values = FileUtils.readLines(FileUtils.getFile
        (adjectivesPath));

    } catch (IOException e) {
      e.printStackTrace();
    }
    return values;
  }
}
