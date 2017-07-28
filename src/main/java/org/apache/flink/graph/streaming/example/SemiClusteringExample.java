/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.streaming.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.library.LabelPropagation;
import org.apache.flink.graph.streaming.library.SemiClustering;
import org.apache.flink.graph.streaming.summaries.LabelPropagationGlobalInfo;
import org.apache.flink.graph.streaming.summaries.SemiClusteringGlobalInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;


public class SemiClusteringExample implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GraphStream<Long, NullValue, NullValue> edges = getGraphStream(env);
        DataStream<SemiClusteringGlobalInfo> lp = edges.aggregate(new SemiClustering(mergeWindowTime, maxVertices, maxClusters, scoreFactor));
        lp.print();
        env.execute("Semi-Clustering");
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgeInputPath = null;
    private static long mergeWindowTime = 1000;
    private static long printWindowTime = 2000;
    private static Long maxVertices = 4l; //每个cluster中可以包含节点的最大数量
    private static Long maxClusters = 2l; //每个TreeSet中可以包含cluster的最大数量
    private static double scoreFactor = 0.5; //分数因子

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 3) {
                System.err.println("Usage: SemiClusteringExample <input edges path> <merge window time (ms)> "
                        + "print window time (ms)");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            mergeWindowTime = Long.parseLong(args[1]);
            printWindowTime = Long.parseLong(args[2]);
        } else {
            System.out.println("Executing SemiClusteringExample example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: SemiClusteringExample <input edges path> <merge window time (ms)> "
                    + "print window time (ms)");
        }
        return true;
    }


    @SuppressWarnings("serial")
    private static GraphStream<Long, NullValue, NullValue> getGraphStream(StreamExecutionEnvironment env) {

        if (fileOutput) {
            return new SimpleEdgeStream<Long, NullValue>(env.readTextFile(edgeInputPath)
                    .map(new MapFunction<String, Edge<Long, NullValue>>() {
                        @Override
                        public Edge<Long, NullValue> map(String s) {
                            String[] fields = s.split("\\s");
                            long src = Long.parseLong(fields[0]);
                            long trg = Long.parseLong(fields[1]);
                            return new Edge<>(src, trg, NullValue.getInstance());
                        }
                    }), env);
        }
//
//        DataStream edges = env.fromElements(
//                new Edge<>(1l, 2l, 1.0),
//                new Edge<>(2l, 3l, 1.0),
//                new Edge<>(1l, 4l, 1.0),
//                new Edge<>(2l, 5l, 1.0),
//                new Edge<>(3l, 4l, 1.0),
//                new Edge<>(3l, 6l, 1.0),
//                new Edge<>(1l, 3l, 1.0),
//                new Edge<>(2l, 4l, 1.0),
//                new Edge<>(5l, 6l, 1.0));

        DataStream edges = env.fromElements(
                new Edge<>(1l, 2l, 1.0),
                new Edge<>(1l, 3l, 1.0),
                new Edge<>(1l, 4l, 1.0),
                new Edge<>(2l, 3l, 1.0),
                new Edge<>(2l, 4l, 1.0),
                new Edge<>(2l, 5l, 1.0),
                new Edge<>(3l, 4l, 1.0),
                new Edge<>(4l, 5l, 1.0),
                new Edge<>(4l, 6l, 1.0),
                new Edge<>(5l, 6l, 1.0),
                new Edge<>(5l, 7l, 1.0),
                new Edge<>(5l, 8l, 1.0),
                new Edge<>(6l, 7l, 1.0),
                new Edge<>(6l, 8l, 1.0),
                new Edge<>(7l, 8l, 1.0));

        return new SimpleEdgeStream<>(edges, env);
    }

    @Override
    public String getDescription() {
        return "Streaming Semi-Clustering";
    }
}