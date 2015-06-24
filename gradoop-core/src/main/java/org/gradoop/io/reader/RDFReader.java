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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.reader;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.EdgeFactory;
import org.gradoop.model.impl.VertexFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads RDF input data serialized as NTriples.
 */
public class RDFReader implements VertexLineReader {
  /**
   * Separates a line into tokens, combinations of whitespace/tab possible.
   */
  private static final Pattern LINE_TOKEN_SEPARATOR =
    Pattern.compile("[ *|\\t*]+");
  /**
   * Remove < and > from single token string having < as first and > as last
   * char.
   */
  private static final Pattern RESOURCE = Pattern.compile("^\\<(.*)\\>$");
  /**
   * Get i.e. 'integer' from
   * "539348"^^<http://www.w3.org/2001/XMLSchema#integer>
   */
  private static final Pattern LITERAL_DATATYPE =
    Pattern.compile("\".*\"\\^\\^.*[#|:]([a-z|A-Z]+)");
  /**
   * Get i.e. '539348' from
   * "539348\"^^<http://www.w3.org/2001/XMLSchema#integer>
   */
  private static final Pattern LITERAL_VALUE = Pattern.compile("\"(.*)\"");
  /**
   * Define MD5 HASH_FUNCTION function.
   */
  private static final HashFunction HASH_FUNCTION = Hashing.md5();
  /**
   * empty string
   */
  private static final String[] EMPTY = {};

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Vertex> readVertexList(String line) {
    String[] triple = simpleValidate(line);
    if (triple.length == 0) {
      return null;
    }
    String s = triple[0];
    String p = triple[1];
    String o = triple[2];
    List<Vertex> vList = Lists.newArrayListWithCapacity(2);

    if (o.startsWith("\"")) { // create one vertex with property
      Vertex vertex = createVertexWithLiteral(s, p, o);
      vList.add(vertex);
    } else { // 2x resource -> 2x vertex
      Vertex sourceVertex = createSourceVertex(s, p, o);
      Vertex targetVertex = createTargetVertex(s, p, o);
      vList.add(sourceVertex);
      vList.add(targetVertex);
    }
    return vList;
  }

  /**
   * Create target vertex for an RDF input line
   * @param s subject
   * @param p predicate
   * @param o object
   * @return resulting vertex
   */
  private Vertex createTargetVertex(String s, String p, String o) {
    Long sourceID = getHash(s);
    Long targetID = getHash(o);
    Long edgeInIndex = getHash(o + p + s);
    Edge edgeIn =
      EdgeFactory.createDefaultEdgeWithLabel(sourceID, p, edgeInIndex);

    return VertexFactory.createDefaultVertex(targetID, o, null, null, Lists
      .newArrayList(edgeIn), null);
  }

  /**
   * Create source vertex for an RDF input line
   * @param s subject (source)
   * @param p predicate
   * @param o object (target)
   * @return resulting vertex
   */
  private Vertex createSourceVertex(String s, String p, String o) {
    Long sourceID = getHash(s);
    Long targetID = getHash(o);
    Long edgeOutIndex = getHash(s + p + o);
    Edge edgeOut =
      EdgeFactory.createDefaultEdgeWithLabel(targetID, p, edgeOutIndex);

    return VertexFactory.createDefaultVertexWithLabel(sourceID, s, Lists
      .newArrayList(edgeOut));
  }

  /**
   * Create vertex  with literal property for an RDF input line
   * @param s subject (target)
   * @param p predicate
   * @param o object (source)
   * @return resulting vertex
   */
  private Vertex createVertexWithLiteral(String s, String p, String o) {
    Long sourceID = getHash(s);
    Map<String, Object> vertexProperties = Maps.newHashMap();
    vertexProperties = addLiteralAsProperty(vertexProperties, p, o);

    return VertexFactory.createDefaultVertex(sourceID,
      s, vertexProperties, null, null, null);
  }

  /**
   * Add a literal to the properties of a vertex
   * @param properties existing properties
   * @param p predicate
   * @param o object (literal value)
   * @return edited properties
   */
  private Map<String, Object> addLiteralAsProperty(
    Map<String, Object> properties, String p, String o) {
    switch (getDatatype(o)) {
    case "integer":
      properties.put(p, Integer.parseInt(getLiteralValue(o)));
      break;
    case "double":
    case "float":
    case "decimal":
      properties.put(p, Double.parseDouble(getLiteralValue(o)));
      break;
    default:
      properties.put(p, getLiteralValue(o));
      break;
    }

    return properties;
  }

  /**
   * Get hash value for UTF8 string.
   * @param value string to be hashed
   * @return resulting hash code
   */
  private Long getHash(String value) {
    return HASH_FUNCTION.newHasher().putString(value, Charsets.UTF_8).hash()
      .asLong();
  }

  /**
   * Simple validation of input line, not complete.
   *
   * @param line single input line
   * @return input line or null for malformed triple
   */
  private String[] simpleValidate(String line) {
    String[] tokens = getTokens(line);
    String s = tokens[0];
    String p = tokens[1];
    String o = tokens[2];

    if (isNTResource(p)) { // predicate has to be URI
      if (isNTResource(s) || s.charAt(0) == '_') {
        //subject has to be blank node or URI
        if (isNTResource(o) || o.charAt(0) == '_' || o.charAt(0) == '"') {
          s = getPlainResource(s);
          p = getPlainResource(p);
          if (isNTResource(o)) {
            o = getPlainResource(o);
          }

          return new String[]{s, p, o};
        } else {
          return EMPTY; // malformed object
        }
      } else {
        return EMPTY; // malformed subject
      }
    } else {
      return EMPTY; // malformed predicate
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsVertexLists() {
    return true;
  }

  /**
   * Separates the whole line using {@code LINE_TOKEN_SEPARATOR}.
   *
   * @param line single input line
   * @return token array
   */
  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  /**
   * Checks token for starting '<' and ending '>'
   *
   * @param token part of a RDF triple
   * @return token without '<' and '>'
   */
  private boolean isNTResource(String token) {
    return RESOURCE.matcher(token).matches();
  }

  /**
   * Retrieves plain string from i.e., <www.example.net>
   *
   * @param token part of a RDF triple
   * @return token without '<' and '>'
   */
  private String getPlainResource(String token) {
    Matcher m = RESOURCE.matcher(token);
    if (m.matches()) {
      return m.group(1);
    }
    return "";
  }

  /**
   * Get data type of RDF literal, if given
   *
   * @param token literal (RDF object)
   * @return extracted data type
   */
  private String getDatatype(String token) {
    Matcher m = LITERAL_DATATYPE.matcher(token);
    if (m.find()) {
      return m.group(1);
    }
    return "";
  }

  /**
   * Get value of RDF literal
   *
   * @param token literal (RDF object)
   * @return literal value
   */
  private String getLiteralValue(String token) {
    Matcher m = LITERAL_VALUE.matcher(token);
    if (m.find()) {
      return m.group(1);
    }
    return "";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(String line) {
    return null;
  }

}
