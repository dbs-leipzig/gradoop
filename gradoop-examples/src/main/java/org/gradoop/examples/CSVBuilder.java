package org.gradoop.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

@SuppressWarnings("Duplicates")
public class CSVBuilder extends AbstractRunner {

    public static void main(String[] args) throws Exception {
        LogicalGraph graph = readLogicalGraph(args[0], "csv");

        DataSet<Vertex> vertices = graph.getVertices();
        DataSet<Edge> edges = graph.getEdges();


        DataSet<Tuple2<Long, Vertex>> idWithVertex = DataSetUtils.zipWithUniqueId(vertices);
        DataSet<Tuple2<Long, Edge>> idWithEdge = DataSetUtils.zipWithUniqueId(edges);


        // <sourceId, edgeId, targetId, edge>
        DataSet<Tuple4<Long, Long, Long, Edge>> idsWithEdge = idWithVertex
                .join(idWithEdge)
                // vertexId == sourceId
                .where("1.id").equalTo("1.sourceId")
                .with(new JoinFunction<Tuple2<Long,Vertex>, Tuple2<Long,Edge>, Tuple3<Long, Long, Edge>>() {
                    @Override
                    public Tuple3<Long, Long, Edge> join(Tuple2<Long, Vertex> v, Tuple2<Long, Edge> e) throws Exception {
                        return Tuple3.of(v.f0, e.f0, e.f1);
                    }
                })
                .join(idWithVertex)
                .where("2.targetId").equalTo("1.id")
                .with(new JoinFunction<Tuple3<Long, Long, Edge>, Tuple2<Long,Vertex>, Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public Tuple4<Long, Long, Long, Edge> join(Tuple3<Long, Long, Edge> vWithSource, Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple4.of(vWithSource.f0, vWithSource.f1, v.f0, vWithSource.f2);
                    }
                });

        // person
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("person");
                    }
                })
                .map(new MapFunction<Tuple2<Long, Vertex>, Tuple6<Long, String, String, Boolean, Boolean, Long>>() {
                    @Override
                    public Tuple6<Long, String, String, Boolean, Boolean, Long> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple6.of(v.f0,
                                v.f1.getPropertyValue("firstName").getString(),
                                v.f1.getPropertyValue("lastName").getString(),
                                v.f1.getPropertyValue("gender").getString().equals("male"),
                                v.f1.getPropertyValue("gender").getString().equals("female"),
                                v.f1.getPropertyValue("birthday").getLong());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/person.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // university
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("university");
                    }
                })
                .map(new MapFunction<Tuple2<Long,Vertex>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple2.of(v.f0, v.f1.getPropertyValue("name").getString());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/university.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // company
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("company");
                    }
                })
                .map(new MapFunction<Tuple2<Long,Vertex>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple2.of(v.f0, v.f1.getPropertyValue("name").getString());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/company.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // city
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("city");
                    }
                })
                .map(new MapFunction<Tuple2<Long,Vertex>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple2.of(v.f0, v.f1.getPropertyValue("name").getString());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/city.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // country
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("country");
                    }
                })
                .map(new MapFunction<Tuple2<Long,Vertex>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple2.of(v.f0, v.f1.getPropertyValue("name").getString());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/country.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // continent
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("continent");
                    }
                })
                .map(new MapFunction<Tuple2<Long,Vertex>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple2.of(v.f0, v.f1.getPropertyValue("name").getString());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/continent.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // tag
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("tag");
                    }
                })
                .map(new MapFunction<Tuple2<Long,Vertex>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple2.of(v.f0, v.f1.getPropertyValue("name").getString());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/tag.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // tagclass
        idWithVertex
                .filter(new FilterFunction<Tuple2<Long, Vertex>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Vertex> v) throws Exception {
                        return v.f1.getLabel().equals("tagclass");
                    }
                })
                .map(new MapFunction<Tuple2<Long,Vertex>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Vertex> v) throws Exception {
                        return Tuple2.of(v.f0, v.f1.getPropertyValue("name").getString());
                    }
                })
                .writeAsCsv(args[1] + "/nodes/tagclass.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // edges

        // knows
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("knows");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple3.of(e.f0, e.f1, e.f2);
                    }
                })
                .writeAsCsv(args[1] + "/rels/knows.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // isLocatedIn
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("isLocatedIn");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple3.of(e.f0, e.f1, e.f2);
                    }
                })
                .writeAsCsv(args[1] + "/rels/isLocatedIn.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // isPartOf
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("isPartOf");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple3.of(e.f0, e.f1, e.f2);
                    }
                })
                .writeAsCsv(args[1] + "/rels/isPartOf.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // hasInterest
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("hasInterest");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple3.of(e.f0, e.f1, e.f2);
                    }
                })
                .writeAsCsv(args[1] + "/rels/hasInterest.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // hasType
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("hasType");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple3.of(e.f0, e.f1, e.f2);
                    }
                })
                .writeAsCsv(args[1] + "/rels/hasType.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // isSubclassOf
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("isSubclassOf");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple3.of(e.f0, e.f1, e.f2);
                    }
                })
                .writeAsCsv(args[1] + "/rels/isSubclassOf.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // studyAt
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("studyAt");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple4<Long, Long, Long, Integer>>() {
                    @Override
                    public Tuple4<Long, Long, Long, Integer> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple4.of(e.f0, e.f1, e.f2, e.f3.getPropertyValue("classYear").getInt());
                    }
                })
                .writeAsCsv(args[1] + "/rels/studyAt.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // workAt
        idsWithEdge
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Edge>>() {
                    @Override
                    public boolean filter(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return e.f3.getLabel().equals("workAt");
                    }
                })
                .map(new MapFunction<Tuple4<Long,Long,Long,Edge>, Tuple4<Long, Long, Long, Integer>>() {
                    @Override
                    public Tuple4<Long, Long, Long, Integer> map(Tuple4<Long, Long, Long, Edge> e) throws Exception {
                        return Tuple4.of(e.f0, e.f1, e.f2, e.f3.getPropertyValue("workFrom").getInt());
                    }
                })
                .writeAsCsv(args[1] + "/rels/workAt.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        getExecutionEnvironment().execute();

//        // person
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/person.csv"))) {
//            writer.write("ID,FIRST_NAME,LAST_NAME,IS_MALE,IS_FEMALE,CREATED_AT");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/person.csv"))) {
//                    stream.forEach(line -> {
//                        try {
//                            writer.newLine();
//                            writer.write(line);
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/person.csv"));
//        }
//
//        // university
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/university.csv"))) {
//            writer.write("ID,NAME");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/university.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/university.csv"));
//        }
//
//        // company
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/company.csv"))) {
//            writer.write("ID,NAME");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/company.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/company.csv"));
//        }
//
//        // city
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/city.csv"))) {
//            writer.write("ID,NAME");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/city.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/city.csv"));
//        }
//
//        // country
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/country.csv"))) {
//            writer.write("ID,NAME");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/country.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/country.csv"));
//        }
//
//        // continent
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/continent.csv"))) {
//            writer.write("ID,NAME");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/continent.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/continent.csv"));
//        }
//
//        // tag
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/tag.csv"))) {
//            writer.write("ID,NAME");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/tag.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/tag.csv"));
//        }
//
//        // tagclass
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/nodes/tagclass.csv"))) {
//            writer.write("ID,NAME");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/nodes/tagclass.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/nodes/tagclass.csv"));
//        }
//
//        // knows
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/knows.csv"))) {
//            writer.write("SRC,ID,DST");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/knows.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/knows.csv"));
//        }
//
//        // isLocatedIn
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/isLocatedIn.csv"))) {
//            writer.write("SRC,ID,DST");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/isLocatedIn.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/isLocatedIn.csv"));
//        }
//
//        // isPartOf
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/isPartOf.csv"))) {
//            writer.write("SRC,ID,DST");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/isPartOf.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/isPartOf.csv"));
//        }
//
//        // hasInterest
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/hasInterest.csv"))) {
//            writer.write("SRC,ID,DST");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/hasInterest.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/hasInterest.csv"));
//        }
//
//        // hasType
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/hasType.csv"))) {
//            writer.write("SRC,ID,DST");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/hasType.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/hasType.csv"));
//        }
//
//        // isSubclassOf
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/isSubclassOf.csv"))) {
//            writer.write("SRC,ID,DST");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/isSubclassOf.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/isSubclassOf.csv"));
//        }
//
//        // studyAt
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/studyAt.csv"))) {
//            writer.write("SRC,ID,DST,CLASS_YEAR");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/studyAt.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/studyAt.csv"));
//        }
//
//        // workAt
//        try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1] + "/rels/workAt.csv"))) {
//            writer.write("SRC,ID,DST,WORK_FROM");
//            try (Stream<String> stream = Files.lines(Paths.get(args[1] + "/rels/workAt.csv"))) {
//                stream.forEach(line -> {
//                    try {
//                        writer.newLine();
//                        writer.write(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            Files.delete(Paths.get(args[1] + "/rels/workAt.csv"));
//        }
    }
}
