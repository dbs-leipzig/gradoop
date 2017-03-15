/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.reader.parsers.rawedges;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

import java.util.List;

/**
 * Represents a raw edge, that is an edge with no ancillary information
 */
public class RawEdge extends ImportEdge<String>
                     implements MapFunction<String, ImportEdge<String>> {

  /**
   * Fixed serializable element
   */
  private static final long serialVersionUID = 6145030L;

  /**
   * Default label for all the edges
   */
  private String label;

  /**
   * Tokenizes strings into numbers
   */
  private NumberTokenizer tokenizer;

  /**
   * Using a default label
   * @param label Default label for each element
   */
  public RawEdge(String label) {
    super();
    this.label = label;
    tokenizer = new NumberTokenizer();
  }

  /**
   * Using a default label, remarking Lapalissian truths
   */
  public RawEdge() {
    this("Edge");
  }

  @Override
  public String getLabel() {
    return label;
  }

  public void setLabel(String defaultLabel) {
    this.label = defaultLabel;
  }

  @Override
  public ImportEdge<String> map(String toParse) throws Exception {
    List<String> elements = this.tokenizer.tokenize(toParse);
    setSourceId(elements.get(0));
    setTargetId(elements.get(1));
    setLabel(label);
    setId(elements.get(0) + "-" + elements.get(1));
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RawEdge)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    RawEdge rawEdge = (RawEdge) o;

    if (label != null ? !label.equals(rawEdge.label) : rawEdge.label != null) {
      return false;
    }
    return tokenizer != null ? tokenizer.equals(rawEdge.tokenizer) : rawEdge.tokenizer == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (label != null ? label.hashCode() : 0);
    result = 31 * result + (tokenizer != null ? tokenizer.hashCode() : 0);
    return result;
  }
}
