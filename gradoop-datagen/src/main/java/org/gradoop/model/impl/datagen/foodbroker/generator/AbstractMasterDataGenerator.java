package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.api.datagen.foodbroker.generator.MasterDataGenerator;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peet on 23.03.16.
 */
public abstract class AbstractMasterDataGenerator<V extends EPGMVertex>
  implements MasterDataGenerator<V> {
  public static final Short GOOD_VALUE = 1;
  public static final Short NORMAL_VALUE = 0;
  public static final Short BAD_VALUE = -1;

  protected final FoodBrokerConfig foodBrokerConfig;
  protected final ExecutionEnvironment env ;
  protected final EPGMVertexFactory<V> vertexFactory;

  public AbstractMasterDataGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig, EPGMVertexFactory<V> vertexFactory) {
    this.foodBrokerConfig = foodBrokerConfig;
    this.env = env;
    this.vertexFactory = vertexFactory;
  }

  protected List<MasterDataSeed> getMasterDataSeeds(String className) {
    Double goodRatio = foodBrokerConfig.getMasterDataGoodRatio(className);

    Double badRatio = foodBrokerConfig.getMasterDataBadRatio(className);

    Integer count = foodBrokerConfig.getMasterDataCount(className);

    Integer goodCount = (int) Math.round(count * goodRatio);
    Integer badCount = (int) Math.round(count * badRatio);
    Integer normalCount = count - goodCount - badCount;

    List<MasterDataSeed> seedList = new ArrayList<>();

    Integer currentId = 0;

    Map<Short, Integer> qualityCounts = new HashMap<>();

    qualityCounts.put(AbstractMasterDataGenerator.GOOD_VALUE, goodCount);
    qualityCounts.put(AbstractMasterDataGenerator.NORMAL_VALUE, normalCount);
    qualityCounts.put(AbstractMasterDataGenerator.BAD_VALUE, badCount);

    for(Map.Entry<Short, Integer> qualityCount : qualityCounts.entrySet()) {

      Short quality = qualityCount.getKey();

      for(int i = 1; i <= qualityCount.getValue(); i++) {
        currentId++;
        seedList.add(new MasterDataSeed(
          currentId, quality
        ));
      }
    }

    Collections.shuffle(seedList);

    return seedList;
  }

  protected List<String> getStringValuesFromFile(String fileName) {
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
