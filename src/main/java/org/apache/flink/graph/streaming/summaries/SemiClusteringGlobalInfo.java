package org.apache.flink.graph.streaming.summaries;


import org.apache.flink.graph.streaming.library.LabelPropagation;

import java.io.Serializable;
import java.util.*;

public class SemiClusteringGlobalInfo implements Serializable {

    private Map<Long, TreeSet<SemiCluster>> clusterMap;
    private Map<Long, HashMap<Long, Double>> neighbors;
    private ClusterScoreComparator comparator;

    private Long maxVertices; //每个cluster中可以包含节点的最大数量
    private Long maxClusters; //每个TreeSet中可以包含cluster的最大数量
    private double scoreFactor; //分数因子

    public SemiClusteringGlobalInfo(Long maxVertices, Long maxClusters, double scoreFactor) {
        clusterMap = new HashMap<>();
        neighbors = new HashMap<>();
        comparator = new ClusterScoreComparator();

        this.maxVertices = maxVertices;
        this.maxClusters = maxClusters;
        this.scoreFactor = scoreFactor;
    }

    public void addEdge(Long vertex1, Long vertex2, Double edgeValue) {

        if (!clusterMap.containsKey(vertex1))
            addVertex(vertex1);
        if (!clusterMap.containsKey(vertex2))
            addVertex(vertex2);

        neighbors.get(vertex1).put(vertex2, edgeValue);
        neighbors.get(vertex2).put(vertex1, edgeValue);

        sendClusters(clusterMap.get(vertex1), vertex2);
        sendClusters(clusterMap.get(vertex2), vertex1);
    }

    public void addVertex(Long vertex) {

//        Double edgeValueSum = 0d;
//        for (Long neighbor : neighbors.get(vertex).keySet())
//            edgeValueSum += neighbors.get(vertex).get(neighbor);
        SemiCluster semiCluster = new SemiCluster(vertex, 1d);

        TreeSet<SemiCluster> initial = new TreeSet();
        initial.add(semiCluster);
        clusterMap.put(vertex, initial);
        neighbors.put(vertex, new HashMap<>());
    }

    public void sendClusters(TreeSet<SemiCluster> clusters, Long receiver) {

        TreeSet<SemiCluster> oriSet = clusterMap.get(receiver);
        TreeSet<SemiCluster> newSet = new TreeSet<>(comparator);
        newSet.addAll(oriSet);

        for (SemiCluster curSemiCluster : clusters) {
            boolean contain = curSemiCluster.vertices.contains(receiver);
            if (!contain && curSemiCluster.vertices.size() < maxVertices) {  //Cluster中不包含当前节点
                SemiCluster newCluster = new SemiCluster(curSemiCluster);
                newCluster.addVertex(receiver, neighbors.get(receiver), scoreFactor);
                if (!containsSemiCluster(newSet, newCluster))
                    newSet.add(newCluster);
            } else if (contain) {
                newSet.add(curSemiCluster);
            }
        }

        Iterator<SemiCluster> iterator = newSet.iterator();
        while (newSet.size() > maxClusters) {  //当超出规定的限额时，把score小的删除
            iterator.next();
            iterator.remove();
        }

        if (needToChange(oriSet, newSet) && newSet.size() != 0) {
            clusterMap.put(receiver, newSet);
            spread(receiver);
        }
    }

    /**
     * 比较两个set里包含的Cluster是否相同
     */
    public boolean needToChange(TreeSet o1, TreeSet o2) {

        if (o1.size() != o2.size())
            return true;

        Iterator<SemiCluster> iterator1 = o1.iterator();
        Iterator<SemiCluster> iterator2 = o2.iterator();

        while (iterator1.hasNext()) {
            if (iterator1.next().compareTo(iterator2.next()) != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 查看一个set是否包含相应的cluster
     */
    public boolean containsSemiCluster(TreeSet<SemiCluster> set, SemiCluster semiCluster) {
        for (SemiCluster sc : set) {
            if (semiCluster.equals(sc))
                return true;
        }
        return false;
    }

    public void spread(Long vertex) {

        for (Long neighbor : neighbors.get(vertex).keySet())
            sendClusters(clusterMap.get(vertex), neighbor);
    }

    public void merge(SemiClusteringGlobalInfo sc) {

        HashSet<Long> needToSpread = new HashSet<>();

        for (Long vertex : sc.clusterMap.keySet()) {
            if (!clusterMap.containsKey(vertex)) {
                clusterMap.put(vertex, sc.clusterMap.get(vertex));
                neighbors.put(vertex, sc.neighbors.get(vertex));
            } else {

                neighbors.get(vertex).putAll(sc.neighbors.get(vertex));

                TreeSet<SemiCluster> oriSet = clusterMap.get(vertex);
                TreeSet<SemiCluster> newSet = new TreeSet<>(comparator);
                newSet.addAll(oriSet);
                TreeSet<SemiCluster> anotherSet = sc.clusterMap.get(vertex);
                newSet.addAll(anotherSet);

                Iterator<SemiCluster> iterator = newSet.iterator();
                while (newSet.size() > maxClusters) {  //当超出规定的限额时，把score小的删除
                    iterator.next();
                    iterator.remove();
                }

                if (needToChange(oriSet, newSet) && newSet.size() != 0) {
                    clusterMap.put(vertex, newSet);
                    needToSpread.add(vertex);
                }
            }
        }

        for (Long vertex : needToSpread)
            spread(vertex);
    }
}
