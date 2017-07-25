package org.apache.flink.graph.streaming.summaries;

import java.io.Serializable;
import java.util.*;


/**
 * 存储最短路径的图数据信息，包含一些对图数据进行处理的方法
 */
public class SSSPGlobalInfo implements Serializable {

    private Map<Long, Double> distance; //key为顶点ID，value为源顶点到当前顶点的最短距离
    private Map<Long, Map<Long, Double>> neighbors; //key为顶点ID，value存储着顶点的邻居信息，其中key为邻居顶点ID，value为两者之间边的权重
    private Long srcVertex;//源顶点

    public SSSPGlobalInfo(long src) {
        distance = new HashMap<>();
        neighbors = new HashMap<>();
        this.srcVertex = src;
        distance.put(srcVertex, 0.0);
        neighbors.put(srcVertex, new HashMap<Long, Double>());
    }

    /**
     * 对边数据进行处理
     * @param v1 边的源顶点
     * @param v2 边的目的顶点
     * @param edgeValue 边的权重
     */
    public void addEdge(Long v1, Long v2, Double edgeValue) {

        if(!distance.containsKey(v1))
            addVertex(v1);
        if(v2 == srcVertex)
            return;
        if(!distance.containsKey(v2))
            addVertex(v2);

        if(!neighbors.get(v1).containsKey(v2))
            neighbors.get(v1).put(v2, edgeValue);

        if(distance.get(v1) < 0 || distance.get(v1) + edgeValue > distance.get(v2) && distance.get(v2) > 0)
            return;

        distance.put(v2, distance.get(v1) + edgeValue);
        spread(v2);
    }

    /**
     * 添加新顶点
     * @param vertex
     */
    public void addVertex(Long vertex) {
        distance.put(vertex, -1d);
        neighbors.put(vertex, new HashMap<Long, Double>());
    }

    /**
     * 对最短路径已改变的顶点值，递归地传递信息给邻居顶点
     * @param vertex
     */
    public void spread(Long vertex) {

        Map<Long, Double> neis = neighbors.get(vertex);
        if(neis == null)
            return;
        for (Long nei : neis.keySet()) {
            if(nei == srcVertex)
                continue;
            if(distance.get(vertex) + neis.get(nei) < distance.get(nei) && distance.get(nei) > 0 || distance.get(nei) < 0) {
                distance.put(nei, distance.get(vertex) + neis.get(nei));
                spread(nei);
            }
        }
    }

    /**
     * 将不同窗口的图数据进行整合，简单的将两个map进行整合，然后对于可能会产生最短距离更新的顶点，判断、更新然后传播
     * @param otherSssp
     */
    public void merge(SSSPGlobalInfo otherSssp) {

        List<Long> needToSpread = new ArrayList<>();
        for (Long vertexInOther : otherSssp.neighbors.keySet()) {
            if(!neighbors.containsKey(vertexInOther))
                neighbors.put(vertexInOther, new HashMap<Long, Double>());
            neighbors.get(vertexInOther).putAll(otherSssp.neighbors.get(vertexInOther));
        }

        for (Long vertexInOther : otherSssp.distance.keySet()) {
            if(distance.containsKey(vertexInOther) && distance.get(vertexInOther) > otherSssp.distance.get(vertexInOther) && otherSssp.distance.get(vertexInOther) > 0) {
                distance.put(vertexInOther, otherSssp.distance.get(vertexInOther));
                needToSpread.add(vertexInOther);
            } else if (!distance.containsKey(vertexInOther))
                distance.put(vertexInOther, otherSssp.distance.get(vertexInOther));
        }

        for (Long vertexInOther : otherSssp.neighbors.keySet()) {
            for (Long vertex : otherSssp.neighbors.get(vertexInOther).keySet()) {
                addEdge(vertexInOther, vertex, otherSssp.neighbors.get(vertexInOther).get(vertex));
            }
        }

        for (Long vertex : needToSpread)
            spread(vertex);
    }

    @Override
    public String toString() {

        StringBuilder res = new StringBuilder("");

        for(Long key: distance.keySet()) {
            res.append("(" + key + ", " + distance.get(key) + ")\n");
        }

        return res.toString();
    }
}
