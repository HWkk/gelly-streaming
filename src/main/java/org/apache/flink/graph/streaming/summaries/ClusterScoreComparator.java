package org.apache.flink.graph.streaming.summaries;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Cluster比较器，主要是根据score进行比较
 */
public class ClusterScoreComparator implements Comparator<SemiCluster>, Serializable {

    @Override
    public int compare(final SemiCluster o1, final SemiCluster o2) {
        if (o1.score < o2.score) {
            return -1;
        } else if (o1.score > o2.score) {
            return 1;
        } else {
            if (!o1.equals(o2)) {
                return 1;
            }
        }
        return 0;
    }
}
