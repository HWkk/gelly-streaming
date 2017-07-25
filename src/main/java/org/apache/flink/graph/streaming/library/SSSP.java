package org.apache.flink.graph.streaming.library;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.summaries.LabelPropagationGlobalInfo;
import org.apache.flink.graph.streaming.summaries.SSSPGlobalInfo;

import java.io.Serializable;

/**
 * 最短路径算法
 */
public class SSSP extends WindowGraphAggregation implements Serializable {

    public SSSP(long mergeWindowTime, long srcVertex) {
        super(new UpdateSSSP(),new CombineSSSP(), new SSSPGlobalInfo(srcVertex), mergeWindowTime, false);
    }

    /**
     * 对每条边数据进行处理
     */
    public final static class UpdateSSSP implements EdgesFold<Long, Double, SSSPGlobalInfo> {

        @Override
        public SSSPGlobalInfo foldEdges(SSSPGlobalInfo sssp, Long vertex, Long vertex2, Double edgeValue) throws Exception {
            sssp.addEdge(vertex, vertex2, edgeValue);
            return sssp;
        }
    }

    /**
     * 对不同窗口的图数据进行整合
     */
    public static class CombineSSSP implements ReduceFunction<SSSPGlobalInfo> {

        @Override
        public SSSPGlobalInfo reduce(SSSPGlobalInfo sssp1, SSSPGlobalInfo sssp2) throws Exception {
            sssp1.merge(sssp2);
            return sssp1;
        }
    }

}
