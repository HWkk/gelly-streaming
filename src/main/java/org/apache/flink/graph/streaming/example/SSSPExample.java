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
import org.apache.flink.graph.streaming.library.SSSP;
import org.apache.flink.graph.streaming.summaries.LabelPropagationGlobalInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.types.NullValue;


public class SSSPExample implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        GraphStream<Long, NullValue, NullValue> edges = getGraphStream(env);
        DataStream<LabelPropagationGlobalInfo> sssp = edges.aggregate(new SSSP(mergeWindowTime, 1));
        sssp.print();
        env.execute("Single Source Shortest Paths");
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgeInputPath = null;
    private static long mergeWindowTime = 1000;
    private static long printWindowTime = 2000;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 3) {
                System.err.println("Usage: SSSPExample <input edges path> <merge window time (ms)> "
                        + "print window time (ms)");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            mergeWindowTime = Long.parseLong(args[1]);
            printWindowTime = Long.parseLong(args[2]);
        } else {
            System.out.println("Executing SSSPExample example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: SSSPExample <input edges path> <merge window time (ms)> "
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

//        DataStream edges = env.fromElements(
//                new Edge<>(1l, 2l, 1.0),
//                new Edge<>(2l, 3l, 1.0),
//                new Edge<>(1l, 4l, 1.0),
//                new Edge<>(2l, 5l, 1.0),
//                new Edge<>(3l, 4l, 1.0),
//                new Edge<>(3l, 6l, 1.0));

        DataStream edges = env.fromElements(
                new Edge<>(1l, 2l, 12.0),
                new Edge<>(1l, 3l, 13.0),
                new Edge<>(2l, 3l, 23.0),
                new Edge<>(3l, 4l, 34.0),
                new Edge<>(3l, 5l, 35.0),
                new Edge<>(4l, 5l, 45.0),
                new Edge<>(5l, 1l, 51.0));

        return new SimpleEdgeStream<>(edges, env);
    }

    @Override
    public String getDescription() {
        return "Streaming Single Source Shortest Path";
    }
}
