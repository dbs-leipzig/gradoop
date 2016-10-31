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

package org.gradoop.flink.io.impl.csv.parser;

import org.gradoop.flink.io.impl.csv.pojos.*;

import javax.xml.bind.annotation.XmlRegistry;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.gradoop.flink.io.impl.csv.pojo package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {


    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.gradoop.flink.io.impl.csv.pojo
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link Static }
     * 
     */
    public Static createStatic() {
        return new Static();
    }

    /**
     * Create an instance of {@link Ref }
     * 
     */
    public Ref createRef() {
        return new Ref();
    }

    /**
     * Create an instance of {@link Reference }
     * 
     */
    public Reference createReference() {
        return new Reference();
    }

    /**
     * Create an instance of {@link Key }
     * 
     */
    public Key createKey() {
        return new Key();
    }

    /**
     * Create an instance of {@link Staticorref }
     * 
     */
    public Staticorref createStaticorref() {
        return new Staticorref();
    }

    /**
     * Create an instance of {@link Properties }
     * 
     */
    public Properties createProperties() {
        return new Properties();
    }

    /**
     * Create an instance of {@link Property }
     * 
     */
    public Property createProperty() {
        return new Property();
    }

    /**
     * Create an instance of {@link Graphs }
     * 
     */
    public Graphs createGraphs() {
        return new Graphs();
    }

    /**
     * Create an instance of {@link Objectreferences }
     * 
     */
    public Objectreferences createObjectreferences() {
        return new Objectreferences();
    }

    /**
     * Create an instance of {@link Target }
     * 
     */
    public Target createTarget() {
        return new Target();
    }

    /**
     * Create an instance of {@link Staticorreference }
     * 
     */
    public Staticorreference createStaticorreference() {
        return new Staticorreference();
    }

    /**
     * Create an instance of {@link Source }
     * 
     */
    public Source createSource() {
        return new Source();
    }

    /**
     * Create an instance of {@link Edges }
     * 
     */
    public Edges createEdges() {
        return new Edges();
    }

    /**
     * Create an instance of {@link Vertexedge }
     * 
     */
    public Vertexedge createVertexedge() {
        return new Vertexedge();
    }

    /**
     * Create an instance of {@link Graphelement }
     * 
     */
    public Graphelement createGraphelement() {
        return new Graphelement();
    }

    /**
     * Create an instance of {@link Element }
     * 
     */
    public Element createElement() {
        return new Element();
    }

    /**
     * Create an instance of {@link Label }
     * 
     */
    public Label createLabel() {
        return new Label();
    }

    /**
     * Create an instance of {@link Edge }
     * 
     */
    public Edge createEdge() {
        return new Edge();
    }

    /**
     * Create an instance of {@link Vertex }
     * 
     */
    public Vertex createVertex() {
        return new Vertex();
    }

    /**
     * Create an instance of {@link Graphhead }
     * 
     */
    public Graphhead createGraphhead() {
        return new Graphhead();
    }

    /**
     * Create an instance of {@link Column }
     * 
     */
    public Column createColumn() {
        return new Column();
    }

    /**
     * Create an instance of {@link Columns }
     * 
     */
    public Columns createColumns() {
        return new Columns();
    }

    /**
     * Create an instance of {@link Csv }
     * 
     */
    public Csv createCsv() {
        return new Csv();
    }

    /**
     * Create an instance of {@link Domain }
     * 
     */
    public Domain createDomain() {
        return new Domain();
    }

    /**
     * Create an instance of {@link Datasource }
     * 
     */
    public Datasource createDatasource() {
        return new Datasource();
    }

}
