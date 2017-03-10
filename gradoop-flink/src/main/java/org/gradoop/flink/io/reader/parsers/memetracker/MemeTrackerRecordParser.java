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

package org.gradoop.flink.io.reader.parsers.memetracker;

import com.google.common.collect.HashMultimap;
import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.AdjacencyListable;
import org.gradoop.flink.io.reader.parsers.inputfilerepresentations.Edgable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Defines a vertex in the MemeTracker graph
 */
public class MemeTrackerRecordParser extends AdjacencyListable<String, MemeTrackerEdge> {

  /**
   * When the vertex is parsed, this fileds contains the default id
   */
  private String id;

  /**
   * When the vertex is parsed, this element is not null and contains the iterator to the set
   */
  private Iterator<String> setIterator;

  /**
   * When the vertex is parsed, this element collects all the outgoing edges
   */
  private Set<String> outgoingEdges;

  public MemeTrackerRecordParser() {
    setIterator = null;
    outgoingEdges = new HashSet<>();
    id = null;
  }

  @Override
  public void updateByParse(String toParse) {
    HashMultimap<MemeProperty, String> propertyVertex = HashMultimap.create();
    for (String row : toParse.split("\n")) {
      MemeProperty t = MemeProperty.fromString(row.substring(0, 1));
      propertyVertex.put(t, row.substring(1).trim());
    }
    for (Map.Entry<MemeProperty, Collection<String>> x : propertyVertex.asMap().entrySet()) {
      switch (x.getKey()) {
      case Id:
        this.id = x.getValue().stream().findFirst().get();
        break;
      case Timestamp:
        this.set(x.toString(), x.getValue().stream().findFirst().get());
        break;
      case Phrase:
        this.set("Phrases", x.getValue().stream().collect(Collectors.joining("\n")));
        break;
      case RefersTo:
        outgoingEdges.addAll(x.getValue());
        setIterator = outgoingEdges.iterator();
        break;
      }
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getLabel() {
    return "WebPage";
  }

  @Override
  public boolean hasNext() {
    return (setIterator != null && setIterator.hasNext());
  }

  @Override
  public MemeTrackerEdge next() {
    return new MemeTrackerEdge(id, setIterator.next());
  }

  @Override
  public void start() {
  }

  @Override
  public void rewind() {
    if (setIterator != null) {
      setIterator = outgoingEdges.iterator();
    }
  }
}
