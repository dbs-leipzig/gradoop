/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.fsm.dimspan.config;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * Frequent subgraph mining configuration.
 */
public class DIMSpanConfig implements Serializable {

  /**
   * support threshold for subgraphs to be considered to be frequenct
   */
  private float minSupport = 1.0f;

  /**
   * Direction mode (true=directed; false=undirected)
   */
  private boolean directed = true;

  /**
   * Dictionary type used for dictionary coding.
   */
  private DictionaryType dictionaryType = DictionaryType.INVERSE_PROPORTIONAL;

  /**
   * Flag to enable embedding compression (true=enabled).
   */
  private boolean embeddingCompressionEnabled = true;

  /**
   * Flag to enable graph compression (true=enabled).
   */
  private boolean graphCompressionEnabled = true;

  /**
   * Flag to set dataflow position of pattern compression.
   */
  private DataflowStep patternCompressionInStep = DataflowStep.MAP;

  /**
   * Flag to set dataflow position of pattern validation.
   */
  private DataflowStep patternVerificationInStep = DataflowStep.COMBINE;

  /**
   * Flag to enable branch constraint in pattern growth (true=enabled).
   */
  private boolean branchConstraintEnabled = true;

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public DIMSpanConfig(float minSupport, boolean directed) {
    this.minSupport = minSupport;
    this.directed = directed;
  }

  @Override
  public String toString() {
    List<String> parameters = Lists.newArrayList();

    parameters.add("s_min : " + String.valueOf(minSupport));
    parameters.add((directed ? "directed" : "undirected") + " mode");
    parameters.add("dictionary type : " + dictionaryType.toString());

    parameters.add(getParameterEnabled("branch constraint", branchConstraintEnabled));
    parameters.add(getParameterEnabled("graph compression", graphCompressionEnabled));
    parameters.add(
      getParameterEnabled("embedding compression", embeddingCompressionEnabled));
    parameters.add("pattern compression @ " + patternCompressionInStep.toString());
    parameters.add("pattern validation @ " + patternVerificationInStep.toString());

    return StringUtils.join(parameters, "|");
  }

  /**
   * Convenience method for string formatting of enum parameters.
   *
   * @param parameter parameter name
   * @param enabled parameter value
   *
   * @return string representation of the parameter and its value
   */
  private String getParameterEnabled(String parameter, boolean enabled) {
    return parameter + " " + (enabled ? "enabled" : "disabled");
  }

  public float getMinSupport() {
    return minSupport;
  }

  public void setMinSupport(float minSupport) {
    this.minSupport = minSupport;
  }

  public boolean isDirected() {
    return directed;
  }

  public void setDirected(boolean directed) {
    this.directed = directed;
  }

  public DictionaryType getDictionaryType() {
    return dictionaryType;
  }

  public void setDictionaryType(DictionaryType dictionaryType) {
    this.dictionaryType = dictionaryType;
  }

  public boolean isEmbeddingCompressionEnabled() {
    return embeddingCompressionEnabled;
  }

  public void setEmbeddingCompressionEnabled(boolean embeddingCompressionEnabled) {
    this.embeddingCompressionEnabled = embeddingCompressionEnabled;
  }

  public boolean isGraphCompressionEnabled() {
    return graphCompressionEnabled;
  }

  public void setGraphCompressionEnabled(boolean graphCompressionEnabled) {
    this.graphCompressionEnabled = graphCompressionEnabled;
  }

  public void setPatternCompressionInStep(DataflowStep patternCompressionInStep) {
    this.patternCompressionInStep = patternCompressionInStep;
  }

  public DataflowStep getPatternCompressionInStep() {
    return patternCompressionInStep;
  }

  public DataflowStep getPatternVerificationInStep() {
    return patternVerificationInStep;
  }

  public void setPatternVerificationInStep(DataflowStep patternVerificationInStep) {
    this.patternVerificationInStep = patternVerificationInStep;
  }

  public void setBranchConstraintEnabled(boolean branchConstraintEnabled) {
    this.branchConstraintEnabled = branchConstraintEnabled;
  }

  public boolean isBranchConstraintEnabled() {
    return branchConstraintEnabled;
  }
}
