/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.Binning;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.util.Util;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_FROM;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.GTE;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

/**
 * Wraps for a graph a set of {@link TemporalElementStats}, one for each combination
 * (vertex / edge) x (label).
 * Provides methods to estimate the probability that comparisons hold, based on
 * the wrapped statistics.
 */
@SuppressWarnings("CanBeFinal")
public class BinningTemporalGraphStatistics extends TemporalGraphStatistics implements Serializable {

  /**
   * Statistics for every vertex label
   */
  private final Map<String, TemporalElementStats> vertexStats;

  /**
   * Statistics for every edge label
   */
  private final Map<String, TemporalElementStats> edgeStats;

  /**
   * Overall vertex count in the graph (exact)
   */
  private Long vertexCount;

  /**
   * Overall edge count in the graph (exact)
   */
  private Long edgeCount;

  /**
   * Maps edge labels to a very rough estimation of their number of distinct
   * source vertices.
   */
  private Map<String, Long> distinctSourceCount;

  /**
   * Maps edge labels to a very rough estimation of their number of distinct
   * target vertices.
   */
  private Map<String, Long> distinctTargetCount;

  /**
   * Set of properties to consider in the estimations.
   * If null, all properties are considered.
   */
  private Set<String> relevantProperties;

  /**
   * Creates new graph statistics
   *
   * @param vertexStats list of vertex statistics (one element for each label)
   * @param edgeStats   list of edge statistics (one element for each label)
   * @param relevantProperties properties to consider
   */
  protected BinningTemporalGraphStatistics(List<TemporalElementStats> vertexStats,
                                           List<TemporalElementStats> edgeStats,
                                           Set<String> relevantProperties) {
    this.vertexStats = new HashMap<>();
    this.edgeStats = new HashMap<>();
    this.vertexCount = 0L;
    this.edgeCount = 0L;
    for (TemporalElementStats edgeStat : edgeStats) {
      this.edgeStats.put(edgeStat.getLabel(), edgeStat);
      this.edgeCount += edgeStat.getElementCount();
    }
    for (TemporalElementStats vertexStat : vertexStats) {
      this.vertexStats.put(vertexStat.getLabel(), vertexStat);
      this.vertexCount += vertexStat.getElementCount();
    }
    this.relevantProperties = relevantProperties;
    initDistinctVertices();
  }

  /**
   * Initializes the statistics for distinct vertices
   */
  private void initDistinctVertices() {
    this.distinctSourceCount = new HashMap<>();
    this.distinctTargetCount = new HashMap<>();

    // for every type of edge...
    for (Map.Entry<String, TemporalElementStats> entries : edgeStats.entrySet()) {
      TemporalElementStats stats = edgeStats.get(entries.getKey());
      List<TemporalElement> sample = stats.getSample();
      int sampleSize = sample.size();
      // ...extract source and target vertex IDs of the edges...
      HashSet<GradoopId> sourceIds = new HashSet<>();
      HashSet<GradoopId> targetIds = new HashSet<>();
      for (TemporalElement edge : sample) {
        sourceIds.add(((TemporalEdge) edge).getSourceId());
        targetIds.add(((TemporalEdge) edge).getTargetId());
      }
      // ... and estimate distinct source and target vertices naively
      long sourceEstimation = sourceIds.size() *
        (stats.getElementCount() / sampleSize);
      long targetEstimation = targetIds.size() *
        (stats.getElementCount() / sampleSize);
      distinctSourceCount.put(entries.getKey(), sourceEstimation);
      distinctTargetCount.put(entries.getKey(), targetEstimation);
    }
  }


  /**
   * Returns the vertex statistics list
   *
   * @return vertex statistics list
   */
  public Map<String, TemporalElementStats> getVertexStats() {
    return vertexStats;
  }

  /**
   * Returns the edge statistics list
   *
   * @return edge statistics list
   */
  public Map<String, TemporalElementStats> getEdgeStats() {
    return edgeStats;
  }

  @Override
  public BinningTemporalGraphStatisticsFactory getFactory() {
    return new BinningTemporalGraphStatisticsFactory();
  }

  @Override
  public long getVertexCount(String label) {
    if (label.isEmpty()) {
      return getVertexCount();
    }
    if (vertexStats.containsKey(label)) {
      return vertexStats.get(label).getElementCount();
    }
    return 0;
  }

  @Override
  public long getVertexCount() {
    return vertexCount;
  }

  @Override
  public long getEdgeCount(String label) {
    if (label.isEmpty()) {
      return edgeCount;
    }
    if (edgeStats.containsKey(label)) {
      return edgeStats.get(label).getElementCount();
    }
    return 0;
  }

  @Override
  public long getEdgeCount() {
    return edgeCount;
  }

  @Override
  public long getDistinctSourceVertexCount(String edgeLabel) {
    return distinctSourceCount.getOrDefault(edgeLabel, 0L);
  }

  @Override
  public long getDistinctSourceVertexCount() {
    return distinctSourceCount.values().stream().reduce(0L, Long::sum);
  }

  @Override
  public long getDistinctTargetVertexCount() {
    return distinctTargetCount.values().stream().reduce(0L, Long::sum);
  }

  @Override
  public long getDistinctTargetVertexCount(String edgeLabel) {
    return distinctTargetCount.getOrDefault(edgeLabel, 0L);
  }

  @Override
  public double estimateTemporalProb(ElementType type1, Optional<String> label1,
                                     TimeSelector.TimeField field1, Comparator comp, Long value) {
    // no further estimations for = and !=
    if (comp == EQ) {
      return 0.001;
    } else if (comp == Comparator.NEQ) {
      return 0.999;
    }
    // <, <=, >=, >
    if (!(label1.isPresent())) {
      return estimateTemporalProb(type1, field1, comp, value);
    } else {
      TemporalElementStats elementStats = (type1 == ElementType.VERTEX) ?
        vertexStats.getOrDefault(label1.get(), null) :
        edgeStats.getOrDefault(label1.get(), null);
      // label not in keys
      if (elementStats == null) {
        return 0.001;
      }
      Binning bins = getBins(elementStats, field1);
      return estimateFromBins(bins, comp, value);
    }
  }

  @Override
  public double estimateTemporalProb(ElementType type1, Optional<String> label1,
                                     TimeSelector.TimeField field1, Comparator comp, ElementType type2,
                                     Optional<String> label2, TimeSelector.TimeField field2) {
    // no further estimations for = and !=
    if (comp == EQ) {
      return 0.001;
    } else if (comp == Comparator.NEQ) {
      return 0.999;
    }
    // stats for lhs (can be more than one, if no label is specified)
    List<TemporalElementStats> statsLhs;
    // same for rhs
    List<TemporalElementStats> statsRhs;

    // determine relevant statistics for rhs and lhs
    if (!(label1.isPresent())) {
      statsLhs = type1 == ElementType.VERTEX ?
        new ArrayList<>(vertexStats.values()) :
        new ArrayList<>(edgeStats.values());
    } else {
      statsLhs = type1 == ElementType.VERTEX ?
        new ArrayList<>(
          Collections.singletonList(vertexStats.getOrDefault(label1.get(), null))) :
        new ArrayList<>(
          Collections.singletonList(edgeStats.getOrDefault(label1.get(), null)));
      // label not in the DB
      if (statsLhs.get(0) == null) {
        return 0.;
      }
    }
    // same for rhs
    if (!(label2.isPresent())) {
      statsRhs = type2 == ElementType.VERTEX ?
        new ArrayList<>(vertexStats.values()) :
        new ArrayList<>(edgeStats.values());
    } else {
      statsRhs = type2 == ElementType.VERTEX ?
        new ArrayList<>(
          Collections.singletonList(vertexStats.getOrDefault(label2.get(), null))) :
        new ArrayList<>(
          Collections.singletonList(edgeStats.getOrDefault(label2.get(), null)));
      // label not in the DB
      if (statsRhs.get(0) == null) {
        return 0.;
      }
    }
    return estimateTimeSelectorComparison(statsLhs, field1, comp, statsRhs, field2);
  }

  /**
   * Estimates the probability that a comparison between two time selectors holds
   * Both selectors are described by the statistics that are relevant for them
   * (one statistic if label was specified, more than one if not).
   *
   * @param statsLhs   list of statistics for the lhs
   * @param fieldLhs   lhs time field (tx_from, tx_to, val_from, val_to)
   * @param comparator comparator of the comparison
   * @param statsRhs   list of statistics for the rhs
   * @param fieldRhs   rhs time field
   * @return estimation of the probability that the comparison holds
   */
  private double estimateTimeSelectorComparison(List<TemporalElementStats> statsLhs,
                                                TimeSelector.TimeField fieldLhs,
                                                Comparator comparator, List<TemporalElementStats> statsRhs,
                                                TimeSelector.TimeField fieldRhs) {
    // count overall number of elements for both sides
    long lhsOverallCount = statsLhs.stream()
      .map(TemporalElementStats::getElementCount)
      .reduce(Long::sum)
      .orElse(0L);
    long rhsOverallCount = statsRhs.stream()
      .map(TemporalElementStats::getElementCount)
      .reduce(Long::sum)
      .orElse(0L);
    double sum = 0.;

    for (TemporalElementStats lhs : statsLhs) {
      double lhsWeight = (double) lhs.getElementCount() / lhsOverallCount;
      for (TemporalElementStats rhs : statsRhs) {
        double rhsWeight = (double) rhs.getElementCount() / rhsOverallCount;
        sum += lhsWeight * rhsWeight *
          estimateTimeSelectorComparison(lhs, fieldLhs, comparator, rhs, fieldRhs);
      }
    }
    return sum;
  }

  /**
   * Estimates the probability that a comparison between two time selectors holds
   * Both selectors are described by the statistics that are relevant for them
   * (one statistic for both lhs and rhs).
   *
   * @param statsLhs   statistics for the lhs
   * @param fieldLhs   lhs time field (tx_from, tx_to, val_from, val_to)
   * @param comparator comparator of the comparison
   * @param statsRhs   statistics for the rhs
   * @param fieldRhs   rhs time field
   * @return estimation of the probability that the comparison holds
   */
  private double estimateTimeSelectorComparison(TemporalElementStats statsLhs,
                                                TimeSelector.TimeField fieldLhs,
                                                Comparator comparator, TemporalElementStats statsRhs,
                                                TimeSelector.TimeField fieldRhs) {

    double sum = 0.;

    // first, compare without considering Long.MIN values, Long.MAX_VALUE values

    double[] lhs = statsLhs.getTemporalPropertyStats(fieldLhs);
    double[] rhs = statsRhs.getTemporalPropertyStats(fieldRhs);
    double lhsMean = lhs[0];
    double lhsVariance = lhs[1];
    double lhsExtreme = lhs[2];
    double rhsMean = rhs[0];
    double rhsVariance = rhs[1];
    double rhsExtreme = rhs[2];
    // probability that both lhs and rhs not Long.MIN_VALUE or Long.MAX_VALUE
    double probBothNotExtreme = (1 - lhsExtreme) * (1 - rhsExtreme);
    // distribution for difference between lhs and rhs values
    NormalDistribution diffDist = new NormalDistribution(lhsMean - rhsMean,
      Math.max(lhsVariance + rhsVariance, 0.001));

    // P(lhs < rhs)
    double probNotExtremeLTE = diffDist.cumulativeProbability(0.);
    // P(lhs = rhs)
    double probNotExtremeEQ = diffDist.probability(0.);
    // P(lhs comp rhs)
    double probCompNotExtreme = 0.;

    if (comparator == LTE) {
      probCompNotExtreme = probNotExtremeLTE;
    } else if (comparator == LT) {
      probCompNotExtreme = probNotExtremeLTE - probNotExtremeEQ;
    } else if (comparator == EQ) {
      probCompNotExtreme = probNotExtremeEQ;
    } else if (comparator == NEQ) {
      probCompNotExtreme = 1. - probNotExtremeEQ;
    } else if (comparator == GTE) {
      probCompNotExtreme = 1. - (probNotExtremeLTE - probNotExtremeEQ);
    } else if (comparator == GT) {
      probCompNotExtreme = 1. - probNotExtremeLTE;
    }

    sum += probBothNotExtreme * probCompNotExtreme;

    // also consider cases in which one or both sides have extreme values
    double probCompExtreme = 0.;
    // lhs from
    if (fieldLhs == TX_FROM || fieldLhs == VAL_FROM) {
      // rhs from
      if (fieldRhs == TX_FROM || fieldRhs == VAL_FROM) {
        if (comparator == EQ) {
          probCompExtreme = lhsExtreme * rhsExtreme;
        } else if (comparator == NEQ) {
          probCompExtreme = (1 - lhsExtreme) * rhsExtreme + lhsExtreme * (1 - rhsExtreme);
        } else if (comparator == LTE) {
          probCompExtreme = lhsExtreme;
        } else if (comparator == LT) {
          probCompExtreme = lhsExtreme * (1 - rhsExtreme);
        } else if (comparator == GTE) {
          probCompExtreme = rhsExtreme;
        } else if (comparator == GT) {
          probCompExtreme = (1 - lhsExtreme) * rhsExtreme;
        }
      } else {
        // rhs to
        if (comparator == NEQ || comparator == LT || comparator == LTE) {
          probCompExtreme = lhsExtreme * rhsExtreme + (1 - lhsExtreme) * rhsExtreme +
            lhsExtreme * (1 - rhsExtreme);
        } //for all other comparators 0, if lhs and/or rhs extreme
      }
    } else { // lhs to
      // rhs from
      if (fieldRhs == TX_FROM || fieldRhs == VAL_FROM) {
        if (comparator == NEQ || comparator == GT || comparator == GTE) {
          probCompExtreme = lhsExtreme * rhsExtreme + (1 - lhsExtreme) * rhsExtreme +
            lhsExtreme * (1 - rhsExtreme);
        } // for all other comparators 0, if lhs and/or rhs extreme
      } else {
        // rhs to
        if (comparator == EQ) {
          probCompExtreme = lhsExtreme * rhsExtreme;
        } else if (comparator == NEQ) {
          probCompExtreme = (1 - lhsExtreme) * rhsExtreme + lhsExtreme * (1 - rhsExtreme);
        } else if (comparator == LTE) {
          probCompExtreme = rhsExtreme;
        } else if (comparator == LT) {
          probCompExtreme = (1 - lhsExtreme) * rhsExtreme;
        } else if (comparator == GTE) {
          probCompExtreme = lhsExtreme;
        } else if (comparator == GT) {
          probCompExtreme = lhsExtreme * (1 - rhsExtreme);
        }
      }
    }

    sum += probCompExtreme;

    return sum;
  }


  @Override
  public double estimateDurationProb(ElementType type, Optional<String> label, Comparator comp,
                                     boolean transaction, Long value) {
    Map<String, TemporalElementStats> statsMap = (type == ElementType.VERTEX) ?
      vertexStats : edgeStats;
    List<TemporalElementStats> relevantStats = label.isPresent() ?
      new ArrayList<>(Arrays.asList(statsMap.get(label.get()))) :
      new ArrayList<>(statsMap.values());

    double sum = 0.;
    long numElements = 0;
    for (TemporalElementStats stat : relevantStats) {
      numElements += stat.getElementCount();
    }

    for (TemporalElementStats stat : relevantStats) {

      double[] durationStats = transaction ?
        stat.getTxDurationStats() : stat.getValDurationStats();
      NormalDistribution durationDist = new NormalDistribution(
        durationStats[0], Math.max(Math.sqrt(durationStats[1]), 0.001));
      double estimation = 0.;

      if (comp == EQ) {
        estimation = durationDist.density(value);
      } else if (comp == NEQ) {
        estimation = 1. - durationDist.density(value);
      } else if (comp == LTE) {
        estimation = durationDist.cumulativeProbability(value);
      } else if (comp == LT) {
        estimation = durationDist.cumulativeProbability(value) -
          durationDist.density(value);
      } else if (comp == GT) {
        estimation = 1 - durationDist.cumulativeProbability(value);
      } else if (comp == GTE) {
        estimation = 1 - (durationDist.cumulativeProbability(value)) +
          durationDist.density(value);
      }
      sum += estimation * ((double) stat.getElementCount() / numElements);
    }
    return sum;
  }

  @Override
  public double estimateDurationProb(ElementType type1, Optional<String> label1, boolean transaction1,
                                     Comparator comp, ElementType type2, Optional<String> label2,
                                     boolean transaction2) {
    Map<String, TemporalElementStats> statsMapLhs = (type1 == ElementType.VERTEX) ?
      vertexStats : edgeStats;
    List<TemporalElementStats> relevantStatsLhs = label1.isPresent() ?
      new ArrayList<>(Collections.singletonList(statsMapLhs.get(label1.get()))) :
      new ArrayList<>(statsMapLhs.values());

    Map<String, TemporalElementStats> statsMapRhs = (type2 == ElementType.VERTEX) ?
      vertexStats : edgeStats;
    List<TemporalElementStats> relevantStatsRhs = label2.isPresent() ?
      new ArrayList<>(Collections.singletonList(statsMapRhs.get(label2.get()))) :
      new ArrayList<>(statsMapRhs.values());

    double sum = 0.;
    long numElements = 0;
    for (TemporalElementStats statL : relevantStatsLhs) {
      for (TemporalElementStats statR : relevantStatsRhs) {
        numElements += statL.getElementCount() * statR.getElementCount();
      }
    }

    for (TemporalElementStats statLhs : relevantStatsLhs) {
      long countLhs = statLhs.getElementCount();

      for (TemporalElementStats statRhs : relevantStatsRhs) {
        long countRhs = statRhs.getElementCount();

        double[] durationStatsLhs = transaction1 ?
          statLhs.getTxDurationStats() : statLhs.getValDurationStats();
        double[] durationStatsRhs = transaction2 ?
          statRhs.getTxDurationStats() : statRhs.getValDurationStats();

        // distribution of differences (LHS - RHS)
        NormalDistribution diffDist = new NormalDistribution(
          durationStatsLhs[0] - durationStatsRhs[0],
          Math.max(Math.sqrt(durationStatsLhs[1] + durationStatsRhs[1]), 0.001));

        double estimation = 0.;

        if (comp == EQ) {
          estimation = diffDist.density(0);
        } else if (comp == NEQ) {
          estimation = 1. - diffDist.density(0);
        } else if (comp == LTE) {
          estimation = diffDist.cumulativeProbability(0);
        } else if (comp == LT) {
          estimation = diffDist.cumulativeProbability(0) -
            diffDist.density(0);
        } else if (comp == GT) {
          estimation = 1 - diffDist.cumulativeProbability(0);
        } else if (comp == GTE) {
          estimation = 1 - (diffDist.cumulativeProbability(0)) +
            diffDist.density(0);
        }

        sum += estimation * ((double) countLhs * (double) countRhs) / (double) numElements;
      }
    }
    return sum;
  }

  @Override
  public double estimatePropertyProb(ElementType type, Optional<String> label, String property,
                                     Comparator comp, PropertyValue value) {
    Map<String, TemporalElementStats> statsMap = type == ElementType.VERTEX ?
      vertexStats : edgeStats;

    List<TemporalElementStats> relevantStats;
    if (label.isPresent()) {
      if (statsMap.containsKey(label.get())) {
        relevantStats = new ArrayList<>(Collections.singletonList(statsMap.get(label.get())));
      } else {
        return 0.001;
      }
    } else {
      relevantStats = new ArrayList<>(statsMap.values());
    }


    if (isNumerical(value)) {
      return estimateNumericalPropertyProb(relevantStats, property, comp, value);
    } else {
      return estimateCategoricalPropertyProb(relevantStats, property, comp, value);
    }

  }

  @Override
  public double estimatePropertyProb(ElementType type1, Optional<String> label1, String property1,
                                     Comparator comp,
                                     ElementType type2, Optional<String> label2, String property2) {
    Map<String, TemporalElementStats> statsMap1 = type1 == ElementType.VERTEX ?
      vertexStats : edgeStats;
    List<TemporalElementStats> relevantStats1 = label1.isPresent() ?
      new ArrayList<>(Collections.singletonList(statsMap1.get(label1.get()))) :
      new ArrayList<>(statsMap1.values());
    Map<String, TemporalElementStats> statsMap2 = type2 == ElementType.VERTEX ?
      vertexStats : edgeStats;
    List<TemporalElementStats> relevantStats2 = label2.isPresent() ?
      new ArrayList<>(Collections.singletonList(statsMap2.get(label2.get()))) :
      new ArrayList<>(statsMap1.values());

    // check if numerical or categorical
    boolean numerical1 = false;
    boolean numerical2 = false;
    for (TemporalElementStats s : relevantStats1) {
      if (s.getNumericalPropertyStatsEstimation().containsKey(property1)) {
        numerical1 = true;
      }
    }
    for (TemporalElementStats s : relevantStats2) {
      if (s.getNumericalPropertyStatsEstimation().containsKey(property2)) {
        numerical2 = true;
      }
    }

    if (isPropertyRelevant(property1) != isPropertyRelevant(property2)) {
      return 0.5;
    }

    if (numerical1 != numerical2) {
      // numerical properties can not be compared to categorical.
      return 0.;
    } else if (!numerical1) {
      return estimateCategoricalPropertyProb(relevantStats1, property1, comp,
        relevantStats2, property2);
    } else {
      return estimateNumericalPropertyProb(relevantStats1, property1, comp,
        relevantStats2, property2);
    }
  }

  /**
   * Estimates the probability that a comparison on numerical property data holds.
   * RHS and LHS elements are described by a list of element statistics
   * Note that the method assumes lhs and rhs to be independent, i.e. a call
   * does not yield a reasonable value for comparing the same property of the
   * exact same element.
   *
   * @param relevantStats1 statistics for lhs elements
   * @param property1      lhs property to compare
   * @param comp           comparator
   * @param relevantStats2 statistics for the rhs elements
   * @param property2      rhs property to compare
   * @return estimation of probability that the comparison holds
   */
  private double estimateNumericalPropertyProb(List<TemporalElementStats> relevantStats1, String property1,
                                               Comparator comp, List<TemporalElementStats> relevantStats2,
                                               String property2) {
    long count1 = relevantStats1.stream()
      .map(TemporalElementStats::getElementCount)
      .reduce(0L, Long::sum);
    long count2 = relevantStats2.stream()
      .map(TemporalElementStats::getElementCount)
      .reduce(0L, Long::sum);
    long totalCount = count1 * count2;

    double outerSum = 0.;
    for (TemporalElementStats s1 : relevantStats1) {
      double innerSum = 0.;
      for (TemporalElementStats s2 : relevantStats2) {
        double prob = estimateNumericalPropertyProb(s1, property1, comp, s2, property2);
        double weight = ((double) s1.getElementCount() * s2.getElementCount()) /
          totalCount;
        innerSum += prob * weight;
      }
      outerSum += innerSum;
    }
    return outerSum;
  }

  /**
   * Estimates the probability that a comparison between numerical properties holds.
   * LHS and RHS are described by one statistics element. They are assumed to be
   * independent, otherwise the estimation may not be reasonable.
   *
   * @param stats1    lhs statistics
   * @param property1 lhs property to compare
   * @param comp      comparator
   * @param stats2    rhs statistics
   * @param property2 rhs property to compare
   * @return estimation of probability that the comparison holds
   */
  private double estimateNumericalPropertyProb(TemporalElementStats stats1, String property1,
                                               Comparator comp,
                                               TemporalElementStats stats2, String property2) {
    Map<String, Double[]> map1 = stats1.getNumericalPropertyStatsEstimation();
    Map<String, Double[]> map2 = stats2.getNumericalPropertyStatsEstimation();
    Double[] propStats1 = map1.getOrDefault(property1, null);
    Double[] propStats2 = map2.getOrDefault(property2, null);
    if (propStats1 == null || propStats2 == null) {
      return 0.0001;
    }
    double occurrence = stats1.getNumericalOccurrenceEstimation()
      .getOrDefault(property1, 0.) *
      stats2.getNumericalOccurrenceEstimation().getOrDefault(property2, 0.);
    // assuming both properties are normally distributed,
    // their difference is also normally distributed
    NormalDistribution differenceDist = new NormalDistribution(
      propStats1[0] - propStats2[0],
      Math.max(Math.sqrt(propStats1[1] + propStats2[1]), 0.001));

    if (comp == EQ) {
      return occurrence * differenceDist.density(0.);
    } else if (comp == NEQ) {
      return occurrence * (1. - differenceDist.density(0.));
    } else if (comp == LTE) {
      return occurrence * (differenceDist.cumulativeProbability(0.));
    } else if (comp == LT) {
      return occurrence * (differenceDist.cumulativeProbability(0.) -
        differenceDist.density(0.));
    } else if (comp == GTE) {
      return occurrence * (1. - differenceDist.cumulativeProbability(0.) +
        differenceDist.density(0.));
    } else {
      //GT
      return occurrence * (1. - differenceDist.cumulativeProbability(0.));
    }
  }

  /**
   * Estimates the probability that a comparison between categorical property values holds.
   * LHS and RHS are described by lists of stats
   *
   * @param relevantStats1 lhs statistics
   * @param property1      lhs property to compare
   * @param comp           comparator
   * @param relevantStats2 rhs statistics
   * @param property2      rhs property to compare
   * @return estimation of the probability that the comparison holds
   */
  private double estimateCategoricalPropertyProb(List<TemporalElementStats> relevantStats1, String property1,
                                                 Comparator comp,
                                                 List<TemporalElementStats> relevantStats2,
                                                 String property2) {
    long count1 = relevantStats1.stream()
      .map(TemporalElementStats::getElementCount)
      .reduce(0L, Long::sum);
    long count2 = relevantStats2.stream()
      .map(TemporalElementStats::getElementCount)
      .reduce(0L, Long::sum);
    long totalCount = count1 * count2;


    double outerSum = 0.;
    for (TemporalElementStats s1 : relevantStats1) {
      double innerSum = 0.;
      for (TemporalElementStats s2 : relevantStats2) {
        double prob = estimateCategoricalPropertyProb(s1, property1, comp, s2, property2);
        double weight = ((double) s1.getElementCount() * s2.getElementCount()) /
          totalCount;
        innerSum += prob * weight;
      }
      outerSum += innerSum;
    }
    return outerSum;
  }

  /**
   * Estimates the probability that a comparison between categorical properties holds.
   * LHS and RHS are both described by a statistics element.
   *
   * @param stats1    lhs statistics
   * @param property1 lhs property to compare
   * @param comp      comparator
   * @param stats2    rhs statistics
   * @param property2 rhs property to compare
   * @return estimation of the probability that the comparison holds
   */
  private double estimateCategoricalPropertyProb(TemporalElementStats stats1, String property1,
                                                 Comparator comp,
                                                 TemporalElementStats stats2, String property2) {
    Map<String, Map<PropertyValue, Double>> map1 =
      stats1.getCategoricalSelectivityEstimation();
    Map<String, Map<PropertyValue, Double>> map2 =
      stats2.getCategoricalSelectivityEstimation();
    Map<PropertyValue, Double> propStats1 = map1.getOrDefault(property1, null);
    Map<PropertyValue, Double> propStats2 = map2.getOrDefault(property2, null);
    // property not sampled or not considered
    if (propStats1 == null || propStats2 == null) {
      // property not considered => return 0.5
      if (!isPropertyRelevant(property1) && !isPropertyRelevant(property2)) {
        return 0.5;
      } else {
        // property not sampled => very rare => return very small value
        return 0.0001;
      }
    }
    if (comp == EQ || comp == NEQ) {
      double sum = 0.;
      for (Map.Entry<PropertyValue, Double> entry1 : propStats1.entrySet()) {
        double val1Selectivity = entry1.getValue();
        for (Map.Entry<PropertyValue, Double> entry2 : propStats2.entrySet()) {
          if (entry1.getKey().equals(entry2.getKey())) {
            double val2Selectivity = entry2.getValue();
            sum += val1Selectivity * val2Selectivity;
          }
        }
      }
      return comp == EQ ? sum : 1. - sum;
    } else {
      // shouldn't happen, categorical variables can only be compared with EQ or NEQ
      return 0.;
    }
  }

  /**
   * Determines if a {@link PropertyValue} is numerical
   *
   * @param value the PropertyValue to check
   * @return true iff the value is of numerical type (Float, Double, Integer, Long)
   */
  private boolean isNumerical(PropertyValue value) {
    Class clz = value.getType();
    return clz.equals(Float.class) || clz.equals(Double.class) ||
      clz.equals(Integer.class) || clz.equals(Long.class);
  }

  /**
   * Estimates the probability that a comparison between a categorical property value
   * and a constant holds
   *
   * @param relevantStats statistics for the lhs (the elements that can have the
   *                      property in question)
   * @param property      the property
   * @param comp          comparator of the comparison
   * @param value         rhs of the comparison
   * @return estimation of the probability that the comparison holds
   */
  private double estimateCategoricalPropertyProb(List<TemporalElementStats> relevantStats, String property,
                                                 Comparator comp, PropertyValue value) {
    if (comp != EQ && comp != NEQ) {
      return 0.;
    }
    long overallCount = 0L;
    boolean found = false;
    for (TemporalElementStats stat : relevantStats) {
      overallCount += stat.getElementCount();
      if (stat.getCategoricalSelectivityEstimation().containsKey(property)) {
        found = true;
      }
    }
    // property not sampled or not considered
    if (!found) {
      // not relevant/considered => simply return 0.5
      if (!isPropertyRelevant(property)) {
        return 0.5;
      } else {
        // not sampled => very rare or not even there => return very small value
        return 0.0001;
      }
    }

    double prob = 0.;
    for (TemporalElementStats stat : relevantStats) {
      if (!stat.getCategoricalSelectivityEstimation().containsKey(property)) {
        // not sampled ? ....
        double factor = 0.0001;
        // ...or just excluded/not relevant?
        if (!isPropertyRelevant(property)) {
          factor = 0.5;
        }
        prob += factor * ((double) stat.getElementCount() / overallCount);
        continue;
      }
      prob += stat.getCategoricalSelectivityEstimation()
        .get(property)
        .getOrDefault(value, 0.0001) *
        ((double) stat.getElementCount() / overallCount);
    }
    if (comp == EQ) {
      return prob;
    } else {
      //NEQ
      return 1 - prob;
    }
  }

  /**
   * Estimates the probability that a comparison between a numerical property value
   * and a numerical constant holds
   *
   * @param relevantStats statistics for the lhs (the elements that can have the
   *                      property in question)
   * @param property      the property
   * @param comp          comparator of the comparison
   * @param value         rhs of the comparison
   * @return estimation of the probability that the comparison holds
   */
  private double estimateNumericalPropertyProb(List<TemporalElementStats> relevantStats, String property,
                                               Comparator comp, PropertyValue value) {
    long overallCount = 0L;
    for (TemporalElementStats stat : relevantStats) {
      overallCount += stat.getElementCount();
    }
    double sum = 0.;
    for (TemporalElementStats stat : relevantStats) {
      sum += estimateNumericalPropertyProb(stat, property, comp, value) *
        (((double) stat.getElementCount() / overallCount));
    }
    return sum;
  }

  /**
   * Estimates the probability that a comparison between a numerical property value
   * and a numerical constant holds for one type of element
   *
   * @param stat          statistics for the lhs
   * @param property      the property
   * @param comp          comparator of the comparison
   * @param value         rhs of the comparison
   * @return estimation of the probability that the comparison holds
   */
  private double estimateNumericalPropertyProb(TemporalElementStats stat, String property,
                                               Comparator comp, PropertyValue value) {
    // property not sampled or not considered
    if (!stat.getNumericalPropertyStatsEstimation().containsKey(property)) {
      // if property not excluded, return very small value
      // if excluded, it is irrelevant, return 0.5
      return isPropertyRelevant(property) ? 0.0001 : 0.5;
    }
    Double[] propertyStats = stat.getNumericalPropertyStatsEstimation().get(property);
    NormalDistribution dist = new NormalDistribution(propertyStats[0],
      Math.max(Math.sqrt(propertyStats[1]), 0.001));
    double doubleValue = Util.propertyValueToDouble(value);
    double occurenceProb = stat.getNumericalOccurrenceEstimation().get(property);
    if (comp == EQ) {
      return 0.001 * occurenceProb;
    } else if (comp == NEQ) {
      return (1. - 0.001) * occurenceProb;
    } else if (comp == LTE) {
      return dist.cumulativeProbability(doubleValue) * occurenceProb;
    } else if (comp == LT) {
      return occurenceProb * (dist.cumulativeProbability(doubleValue) - 0.001);
    } else if (comp == GTE) {
      return occurenceProb *
        (1. - dist.cumulativeProbability(doubleValue) + 0.001);
    } else {
      //GT
      return occurenceProb * (1. - dist.cumulativeProbability(doubleValue));
    }
  }

  /**
   * Estimates probability of a comparison for elements without label (i.e. probability is determined
   * over all vertices/edges)
   *
   * @param type1  type of the comparison's lhs (vertex/edge)
   * @param field1 field of the lhs (tx_from, tx_to, valid_from, valid_to)
   * @param comp   comparator
   * @param value  rhs (constant value)
   * @return estimation of probability that the specified comparison holds
   */
  private double estimateTemporalProb(ElementType type1, TimeSelector.TimeField field1,
                                      Comparator comp, Long value) {
    double sum = 0.;
    Map<String, TemporalElementStats> stats = type1 == ElementType.VERTEX ?
      vertexStats : edgeStats;

    Long overallCount = type1 == ElementType.VERTEX ? vertexCount : edgeCount;
    for (TemporalElementStats elementStat : stats.values()) {
      Binning bins = getBins(elementStat, field1);
      sum += estimateFromBins(bins, comp, value) *
        (elementStat.getElementCount() / (double) overallCount);
    }
    return sum;
  }

  /**
   * Estimates the probability that a comparison holds using bin statistics
   *
   * @param bins  the bins that describe the lhs of the comparison
   * @param comp  comparator, not EQ or NEQ
   * @param value rhs of the comparison
   * @return estimation of probability that the comparison holds
   */
  private double estimateFromBins(Binning bins, Comparator comp, Long value) {
    int[] valueBins = findBins(bins, value);
    int numBins = bins.getBins().length;

    if (comp == Comparator.LT) {
      return ((double) valueBins[0]) / ((double) numBins);
    } else if (comp == LTE) {
      return ((double) valueBins[1]) / ((double) numBins);
    } else if (comp == Comparator.GT) {
      // 1 - LTE estimation
      return 1 - ((double) valueBins[1]) / ((double) numBins);
    } else {
      // 1- LT estimation for GTE
      return 1 - ((double) valueBins[0]) / ((double) numBins);
    }
  }

  /**
   * Checks whether a property key should be considered.
   * This is the case iff no property keys to consider are specified
   * (then all are relevant) or the set of specified relevant key contains
   * the property
   * @param property property to check for relevance
   * @return true iff property should be considered for estimations
   */
  private boolean isPropertyRelevant(String property) {
    return relevantProperties == null || relevantProperties.contains(property);
  }

  /**
   * Finds the bins in which a value falls
   *
   * @param bins  bins to search for the value
   * @param value value to search
   * @return integer array containing the lowest suitable bin index and the highest.
   * A value can fall into more than one bin in a row, as the bins are equal width.
   * E.g., binning a sequence {@code 1,2,2,3} into 4 bins would yield 2 suitable bins
   * for the value {@code 2}
   */
  private int[] findBins(Binning bins, Long value) {
    Long[] binReps = bins.getBins();
    int bin = Arrays.binarySearch(binReps, value);
    if (bin < 0) {
      bin = -bin;
    }
    bin = Math.min(bin, binReps.length - 1);
    // bins are equal size, so the same bin could occur more than once
    // the lowest bin in which value fits
    int minBin = bin;
    // the "highest" bin in which value fits
    int maxBin = bin;
    while (minBin >= 0 && matchesBin(binReps, value, minBin)) {
      minBin--;
    }
    minBin++;
    while (maxBin < binReps.length && matchesBin(binReps, value, maxBin)) {
      maxBin++;
    }
    maxBin--;
    return new int[] {minBin, maxBin};
  }

  /**
   * Checks whether a suitable bin was found for a value.
   *
   * @param arr   the array of bins
   * @param value the value to search
   * @param index index of the potentially suitable bin
   * @return true iff index denotes a suitable bin for value
   */
  private boolean matchesBin(Long[] arr, Long value, int index) {
    if (arr.length == 1) {
      return true;
    }
    if (index == arr.length - 1) {
      return value >= arr[index];
    } else if (index == 0) {
      return value < arr[index + 1];
    } else {
      boolean lowerCond = value >= arr[index];
      boolean upperCond = value == Long.MAX_VALUE ? value <= arr[index + 1] :
        value < arr[index + 1];
      return lowerCond && upperCond;
    }
  }

  /**
   * Auxiliary method to get the bin for a certain time field (tx_from, tx_to, valid_from, valid_to)
   *
   * @param elementStats element statistics from which to retrieve the bin
   * @param field1       time field
   * @return binning statistics for the time field
   */
  private Binning getBins(TemporalElementStats elementStats, TimeSelector.TimeField field1) {
    if (field1 == TimeSelector.TimeField.TX_FROM) {
      return elementStats.getEstimatedTimeBins()[0];
    } else if (field1 == TX_TO) {
      return elementStats
        .getEstimatedTimeBins()[1];
    } else if (field1 == TimeSelector.TimeField.VAL_FROM) {
      return elementStats.getEstimatedTimeBins()[2];
    } else {
      return elementStats.getEstimatedTimeBins()[3];
    }
  }

}
