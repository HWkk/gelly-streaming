package org.apache.flink.graph.streaming.library;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.summaries.SSSPGlobalInfo;
import org.apache.flink.graph.streaming.summaries.SemiClusteringGlobalInfo;

import java.io.Serializable;

/**
 * Semi-Clustering算法
 */
public class SemiClustering extends WindowGraphAggregation implements Serializable {

    public SemiClustering(long mergeWindowTime, Long maxVertices, Long maxClusters, double scoreFactor) {
        super(new UpdateSemiClustering(),new CombineSemiClustering(), new SemiClusteringGlobalInfo(maxVertices, maxClusters, scoreFactor), mergeWindowTime, false);
    }

    /**
     * 对每条边数据进行处理
     */
    public final static class UpdateSemiClustering implements EdgesFold<Long, Double, SemiClusteringGlobalInfo> {

        @Override
        public SemiClusteringGlobalInfo foldEdges(SemiClusteringGlobalInfo sc, Long vertex, Long vertex2, Double edgeValue) throws Exception {
            sc.addEdge(vertex, vertex2, edgeValue);
            return sc;
        }
    }

    /**
     * 对不同窗口的图数据进行整合
     */
    public static class CombineSemiClustering implements ReduceFunction<SemiClusteringGlobalInfo> {

        @Override
        public SemiClusteringGlobalInfo reduce(SemiClusteringGlobalInfo sc1, SemiClusteringGlobalInfo sc2) throws Exception {
            sc1.merge(sc2);
            return sc1;
        }
    }
}
