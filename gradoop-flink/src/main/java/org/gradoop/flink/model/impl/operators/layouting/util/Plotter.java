/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.BasicStroke;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * DataSink to write a layouted graph to an image
 */

public class Plotter implements DataSink, Serializable {

  /**
   * ImageIO-format used for intermediate image-encodings
   */
  protected static final String INTERMEDIATE_ENCODING = "png";

  /**
   * Path to store the output-image
   */
  protected String path;
  /**
   * Width of the original layout of the graph
   */
  protected int layoutWidth;
  /**
   * Height of the original layout of the graph
   */
  protected int layoutHeight;
  /**
   * Requested width of output-image (px)
   */
  protected int imageWidth;
  /**
   * Requested height of output-image (px)
   */
  protected int imageHeight;

  /**
   * Size of the vertex-symbols (px)
   */
  protected int vertexSize = 10;
  /**
   * Size (width) of egde-lines (px)
   */
  protected float edgeSize = 1f;
  /**
   * Color of vertices
   */
  protected Color vertexColor = Color.RED;
  /**
   * Color of edges
   */
  protected Color edgeColor = Color.WHITE;
  /**
   * Color of the background
   */
  protected Color backgroundColor = Color.BLACK;
  /**
   * If true, do not draw vertices, only edges. Improves performance.
   */
  protected boolean ignoreVertices = false;
  /**
   * Name of the property that should be drawn as vertex 'heading'. If null, don't draw anything
   */
  protected String vertexLabel = null;
  /**
   * Font-size of the vertex-heading
   */
  protected int vertexLabelSize = 10;
  /**
   * If true, use SIZE-Property to dynamically choose vertex-size.
   */
  protected boolean dynamicVertexSize = false;
  /**
   * If true, use SIZE-Property to dynamically choose vertex-size.
   */
  protected boolean dynamicEdgeSize = false;
  /**
   * If true, scale the layout to fill the complete drawing-space.
   */
  protected boolean zoom = false;

  /**
   * Create new plotter.
   *
   * @param path         Target-path for image
   * @param layoutWidth  Width of the graph-layout
   * @param layoutHeight Height of the graph-layout
   * @param imageWidth   Wanted width of the output image
   * @param imageHeight  Wanted height of the output image
   */
  public Plotter(String path, int layoutWidth, int layoutHeight, int imageWidth, int imageHeight) {
    this.path = path;
    this.layoutWidth = layoutWidth;
    this.layoutHeight = layoutHeight;
    this.imageWidth = imageWidth;
    this.imageHeight = imageHeight;
  }

  /**
   * Create new plotter.
   *
   * @param path        Target-path for image
   * @param algo        Layouting algorithm used to create the layout. IS used to determine
   *                    layout width
   *                    and height.
   * @param imageWidth  Wanted width of the output image
   * @param imageHeight Wanted height of the output image
   */
  public Plotter(String path, LayoutingAlgorithm algo, int imageWidth, int imageHeight) {
    this(path, algo.getWidth(), algo.getHeight(), imageWidth, imageHeight);
  }

  /**
   * Sets optional value vertexSize
   *
   * @param vertexSize the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexSize(int vertexSize) {
    this.vertexSize = vertexSize;
    return this;
  }

  /**
   * Sets optional value vertexColor
   *
   * @param vertexColor the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexColor(Color vertexColor) {
    this.vertexColor = vertexColor;
    return this;
  }

  /**
   * Sets optional value edgeColor
   *
   * @param edgeColor the new value
   * @return this (for method-chaining)
   */
  public Plotter edgeColor(Color edgeColor) {
    this.edgeColor = edgeColor;
    return this;
  }

  /**
   * Sets optional value ignoreVertices
   *
   * @param ignoreVertices the new value
   * @return this (for method-chaining)
   */
  public Plotter ignoreVertices(boolean ignoreVertices) {
    this.ignoreVertices = ignoreVertices;
    return this;
  }

  /**
   * Sets optional value vertexLabel
   *
   * @param vertexLabel the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexLabel(String vertexLabel) {
    this.vertexLabel = vertexLabel;
    return this;
  }

  /**
   * Sets optional value vertexLabelSize
   *
   * @param vertexLabelSize the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexLabelSize(int vertexLabelSize) {
    this.vertexLabelSize = vertexLabelSize;
    return this;
  }

  /**
   * Sets optional value backgroundColor
   *
   * @param backgroundColor the new value
   * @return this (for method-chaining)
   */
  public Plotter backgroundColor(Color backgroundColor) {
    this.backgroundColor = backgroundColor;
    return this;
  }

  /**
   * Sets optional value edgeSize
   *
   * @param edgeSize the new value
   * @return this (for method-chaining)
   */
  public Plotter edgeSize(float edgeSize) {
    this.edgeSize = edgeSize;
    return this;
  }

  /**
   * Sets optional value dynamicVertexSize
   *
   * @param dynamicVertexSize the new value
   * @return this (for method-chaining)
   */
  public Plotter dynamicVertexSize(boolean dynamicVertexSize) {
    this.dynamicVertexSize = dynamicVertexSize;
    return this;
  }

  /**
   * Sets optional value dynamicEdgeSize
   *
   * @param dynamicEdgeSize the new value
   * @return this (for method-chaining)
   */
  public Plotter dynamicEdgeSize(boolean dynamicEdgeSize) {
    this.dynamicEdgeSize = dynamicEdgeSize;
    return this;
  }

  /**
   * If true, scale the graph to completely fill the layout-area
   *
   * @param zoom the new value
   * @return this (for method-chaining)
   */
  public Plotter zoom(boolean zoom) {
    this.zoom = zoom;
    return this;
  }

  /**
   * Prepare the given edges for drawing. Assign them start- and end-coordinates from their
   * vertices.
   *
   * @param vertices The vertices to take the edge-coordinates from
   * @param edges    The raw edges
   * @return The prepared edges
   */
  protected DataSet<EPGMEdge> prepareEdges(DataSet<EPGMVertex> vertices, DataSet<EPGMEdge> edges) {
    edges = edges.join(vertices).where("sourceId").equalTo("id")
      .with(new JoinFunction<EPGMEdge, EPGMVertex, EPGMEdge>() {
        public EPGMEdge join(EPGMEdge first, EPGMVertex second) throws Exception {
          first.setProperty("source_x", second.getPropertyValue("X"));
          first.setProperty("source_y", second.getPropertyValue("Y"));
          return first;
        }
      }).join(vertices).where("targetId").equalTo("id")
      .with(new JoinFunction<EPGMEdge, EPGMVertex, EPGMEdge>() {
        public EPGMEdge join(EPGMEdge first, EPGMVertex second) throws Exception {
          first.setProperty("target_x", second.getPropertyValue("X"));
          first.setProperty("target_y", second.getPropertyValue("Y"));
          return first;
        }
      });
    return edges;
  }

  /**
   * Scale the coordinates of the graph so that the layout-space matches the requested drawing-size
   *
   * @param inp original vertices
   * @return vertices with scaled coordinates
   */
  protected DataSet<EPGMVertex> scaleLayout(DataSet<EPGMVertex> inp) {

    if (zoom) {
      final int imageWidthF = imageWidth;
      final int imageHeightF = imageHeight;

      DataSet<Tuple4<Integer, Integer, Integer, Integer>> minMaxCoords = inp.map((v) -> {
        int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
        int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
        return new Tuple4<>(x, y, x, y);
      }).returns(new TypeHint<Tuple4<Integer, Integer, Integer, Integer>>() {
      }).aggregate(Aggregations.MIN, 0).and(Aggregations.MIN, 1).and(Aggregations.MAX, 2)
        .and(Aggregations.MAX, 3);

      return inp.map(new RichMapFunction<EPGMVertex, EPGMVertex>() {
        private int offsetX = 0;
        private int offsetY = 0;
        private double zoomFactor = 1;

        @Override
        public void open(Configuration parameters) throws Exception {
          super.open(parameters);
          List<Tuple4<Integer, Integer, Integer, Integer>> minmaxlist =
            getRuntimeContext().getBroadcastVariable("MINMAX");
          offsetX = minmaxlist.get(0).f0;
          offsetY = minmaxlist.get(0).f1;
          int maxX = minmaxlist.get(0).f2;
          int maxY = minmaxlist.get(0).f3;
          int xRange = maxX - offsetX;
          int yRange = maxY - offsetY;
          zoomFactor =
            (xRange > yRange) ? imageWidthF / (double) xRange : imageHeightF / (double) yRange;
        }

        @Override
        public EPGMVertex map(EPGMVertex v) {
          int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
          int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
          x = (int) ((x - offsetX) * zoomFactor);
          y = (int) ((y - offsetY) * zoomFactor);
          v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY, x);
          v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY, y);
          return v;
        }
      }).withBroadcastSet(minMaxCoords, "MINMAX");

    } else {

      final double widthScale = imageWidth / (double) layoutHeight;
      final double heightScale = imageHeight / (double) layoutHeight;
      return inp.map((v) -> {
        int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
        int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
        x = (int) (x * widthScale);
        y = (int) (y * heightScale);
        v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY, x);
        v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY, y);
        return v;
      });

    }
  }

  /**
   * Convert a BufferedImage to byte[]
   *
   * @param img The image to convert
   * @return byte[] representation of the image
   */
  protected static byte[] imgToArr(BufferedImage img) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ImageIO.write(img, INTERMEDIATE_ENCODING, baos);
      return baos.toByteArray();
    } catch (IOException e) {
      //can not happen
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Convert byte[] to BufferedImage
   *
   * @param arr The array to convert
   * @return The buffered-image representation
   */
  protected static BufferedImage arrToImg(byte[] arr) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(arr);
      return ImageIO.read(bais);
    } catch (IOException e) {
      //can not happen
      e.printStackTrace();
    }
    return null;
  }


  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, true);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, true);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {

    ImageOutputFormat pof = new ImageOutputFormat(path);
    FileSystem.WriteMode writeMode =
      overwrite ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;
    pof.setWriteMode(writeMode);

    DataSet<EPGMVertex> vertices = scaleLayout(logicalGraph.getVertices());
    DataSet<EPGMEdge> edges = prepareEdges(vertices, logicalGraph.getEdges());

    ImageGenerator imgg = new ImageGenerator(this);
    DataSet<byte[]> image = edges.combineGroup(imgg::combineEdges).reduce(imgg::mergeImages);
    if (!ignoreVertices) {
      DataSet<byte[]> vertexImage =
        vertices.combineGroup(imgg::combineVertices).reduce(imgg::mergeImages);
      image = image.map(new RichMapFunction<byte[], byte[]>() {
        @Override
        public byte[] map(byte[] bufferedImage) throws Exception {
          List<byte[]> vertexImage = this.getRuntimeContext().getBroadcastVariable("vertexImage");
          return imgg.mergeImages(bufferedImage, vertexImage.get(0));
        }
      }).withBroadcastSet(vertexImage, "vertexImage");
    }
    image = image.map(imgg::addBackgound);

    image.output(pof).setParallelism(1);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    throw new UnsupportedOperationException("Plotting is not supported for GraphCollections");
  }

  /**
   * This class contains functionality to create images from graph-parts.
   * For some strange reasons BufferedImage can not be used as DataSet-Type without crashing the
   * JVM. Therefore byte[] is used as intermediate-representation.
   */
  protected static class ImageGenerator implements Serializable {

    /**
     * Contains all necessary parameters
     */
    private Plotter plotter;

    /**
     * Create new image-generatir
     *
     * @param p Contains all necessary parameters (cannot use non-static class du to flink-madness)
     */
    public ImageGenerator(Plotter p) {
      this.plotter = p;
    }


    /**
     * Combine multiple edges into one Image
     *
     * @param iterable  The edges to combine
     * @param collector The output-collector
     */
    public void combineEdges(Iterable<EPGMEdge> iterable, Collector<byte[]> collector) {
      BufferedImage img =
        new BufferedImage(plotter.imageWidth, plotter.imageHeight, BufferedImage.TYPE_INT_ARGB);
      Graphics2D gfx = img.createGraphics();
      gfx.setColor(plotter.edgeColor);
      gfx.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
      gfx.setStroke(new BasicStroke(plotter.edgeSize));
      for (EPGMEdge e : iterable) {
        drawEdge(gfx, e);
      }
      collector.collect(imgToArr(img));
      gfx.dispose();
    }

    /**
     * Combine multiple vertices into one Image
     *
     * @param iterable  The vertices to combine
     * @param collector The output-collector
     */
    public void combineVertices(Iterable<EPGMVertex> iterable, Collector<byte[]> collector) {
      BufferedImage img =
        new BufferedImage(plotter.imageWidth, plotter.imageHeight, BufferedImage.TYPE_INT_ARGB);
      Graphics2D gfx = img.createGraphics();
      gfx.setColor(plotter.vertexColor);
      for (EPGMVertex v : iterable) {
        drawVertex(gfx, v);
      }
      collector.collect(imgToArr(img));
      gfx.dispose();
    }

    /**
     * Draw a single edge
     *
     * @param gfx The graphics-object to use for drawing
     * @param e   The edge to draw
     */
    private void drawEdge(Graphics2D gfx, EPGMEdge e) {
      gfx.setColor(plotter.edgeColor);
      float edgeSize = plotter.edgeSize;
      if (plotter.dynamicEdgeSize && e.getPropertyValue("SIZE") != null) {
        edgeSize *= Math.sqrt((float) e.getPropertyValue("SIZE").getInt());
      }
      gfx.setStroke(new BasicStroke(edgeSize));
      try {
        int sourceX = e.getPropertyValue("source_x").getInt();
        int sourceY = e.getPropertyValue("source_y").getInt();

        int targetX = e.getPropertyValue("target_x").getInt();
        int targetY = e.getPropertyValue("target_y").getInt();

        gfx.drawLine(sourceX, sourceY, targetX, targetY);
      } catch (NullPointerException ef) {

      }
    }

    /**
     * Draw a single vertex
     *
     * @param gfx The graphics-object to use for drawing
     * @param v   The vertex to draw
     */
    private void drawVertex(Graphics2D gfx, EPGMVertex v) {
      int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
      int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
      int size = plotter.vertexSize;
      if (plotter.dynamicVertexSize && v.getPropertyValue("SIZE") != null) {
        size *= Math.sqrt((double) v.getPropertyValue("SIZE").getInt());
      }
      gfx.fillOval(x - size / 2, y - size / 2, size, size);
      if (plotter.vertexLabel != null) {
        String label = v.getPropertyValue(plotter.vertexLabel).getString();
        gfx.drawString(label, x, y + (plotter.vertexSize) + 10 + (plotter.vertexLabelSize / 2));
      }
    }

    /**
     * Merge two intermediate Images into one
     *
     * @param arr1 Image 1
     * @param arr2 Image 2
     * @return Output-Image
     */
    public byte[] mergeImages(byte[] arr1, byte[] arr2) {
      BufferedImage bufferedImage = arrToImg(arr1);
      BufferedImage t1 = arrToImg(arr2);
      Graphics2D g = bufferedImage.createGraphics();
      g.drawImage(t1, 0, 0, plotter.imageWidth, plotter.imageHeight, null);
      g.dispose();
      return imgToArr(bufferedImage);
    }

    /**
     * Draw a black background behind the image
     *
     * @param arr Input image
     * @return Input-image + black background
     */
    public byte[] addBackgound(byte[] arr) {
      BufferedImage bufferedImage = arrToImg(arr);
      BufferedImage out =
        new BufferedImage(plotter.imageWidth, plotter.imageHeight, BufferedImage.TYPE_INT_ARGB);
      Graphics2D gfx = out.createGraphics();
      gfx.setColor(plotter.backgroundColor);
      gfx.fillRect(0, 0, plotter.imageWidth, plotter.imageHeight);
      gfx.drawImage(bufferedImage, 0, 0, plotter.imageWidth, plotter.imageHeight, null);
      gfx.dispose();
      return imgToArr(out);
    }
  }

  /**
   * OutputFormat to save BufferedImages to image files
   */
  protected static class ImageOutputFormat extends FileOutputFormat<byte[]> {

    /**
     * Where to store the output-image
     */
    private String path;

    /**
     * Create a new plotter output format
     *
     * @param path The output-image location
     */
    public ImageOutputFormat(String path) {
      super(new Path(path));
      this.path = path;
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
      super.open(taskNumber, numTasks);
    }

    /**
     * Get the file extension of a file
     *
     * @param path The name/path of the file
     * @return The extension (without dot)
     */
    private String getFileExtension(String path) {
      return path.substring(path.lastIndexOf('.') + 1);
    }


    @Override
    public void writeRecord(byte[] img) throws IOException {
      String outputFormat = getFileExtension(path);
      if (outputFormat != INTERMEDIATE_ENCODING) {
        BufferedImage bimg = arrToImg(img);
        ImageIO.write(bimg, outputFormat, this.stream);
      } else {
        this.stream.write(img);
      }

    }

  }

}
