package org.apache.flink.graph.streaming.summaries;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 * 存储标签传播算法的图数据信息，包含一些对图数据进行处理的方法
 */
public class LabelPropagationGlobalInfo implements Serializable {

    private Map<Long, Long> communityMap; //key为顶点ID，value为顶点所属社区ID
    private Map<Long, Map<Long, Long>> neighborCommunityMap; //key为顶点ID，value存储着邻居顶点所属的社区频次，其中key为邻居社区ID，value为频次
    private Map<Long, Set<Long>> neighbors;//key为顶点ID，value保存着邻居顶点ID

    public LabelPropagationGlobalInfo() {
        communityMap = new HashMap<>();
        neighborCommunityMap = new HashMap<>();
        neighbors = new HashMap<>();
    }

    /**
     * 对新添加的边数据进行处理
     * @param v1 源顶点
     * @param v2 目的顶点
     */
    public void addEdge(Long v1, Long v2) {

        if (!communityMap.containsKey(v1))
            addVertex(v1);
        if (!communityMap.containsKey(v2))
            addVertex(v2);

        if (!neighbors.get(v1).contains(v2) || !neighbors.get(v2).contains(v1)) {
            neighbors.get(v1).add(v2);
            neighbors.get(v2).add(v1);
            initialNeighbors(v1, v2);
            changeCommunityIfNeed(v1, communityMap.get(v2));
            changeCommunityIfNeed(v2, communityMap.get(v1));
        }
    }

    /**
     * 添加顶点
     * @param vertex
     */
    public void addVertex(Long vertex) {
        communityMap.put(vertex, vertex);
        neighborCommunityMap.put(vertex, new HashMap<Long, Long>());
        neighborCommunityMap.get(vertex).put(vertex, 0l);
        neighbors.put(vertex, new HashSet<Long>());
    }

    public void initialNeighbors(Long v1, Long v2) {
        if (!neighborCommunityMap.get(v1).containsKey(communityMap.get(v2)))
            neighborCommunityMap.get(v1).put(communityMap.get(v2), 1l);
        else
            neighborCommunityMap.get(v1).put(communityMap.get(v2), neighborCommunityMap.get(v1).get(communityMap.get(v2)) + 1);
        if (!neighborCommunityMap.get(v2).containsKey(communityMap.get(v1)))
            neighborCommunityMap.get(v2).put(communityMap.get(v1), 1l);
        else
            neighborCommunityMap.get(v2).put(communityMap.get(v1), neighborCommunityMap.get(v2).get(communityMap.get(v1)) + 1);
    }

    /**
     * 更新顶点的邻居社区频次
     * @param vertex 顶点
     * @param oriCommunity 原社区
     * @param curCommunity 新社区
     */
    public void updateCommunityNumber(Long vertex, Long oriCommunity, Long curCommunity) {

        if(!neighborCommunityMap.get(vertex).containsKey(oriCommunity))
            neighborCommunityMap.get(vertex).put(oriCommunity, 1l);
        neighborCommunityMap.get(vertex).put(oriCommunity, neighborCommunityMap.get(vertex).get(oriCommunity) - 1);

        if (neighborCommunityMap.get(vertex).containsKey(curCommunity))
            neighborCommunityMap.get(vertex).put(curCommunity, neighborCommunityMap.get(vertex).get(curCommunity) + 1);
        else
            neighborCommunityMap.get(vertex).put(curCommunity, 1l);
    }

    /**
     * 判断是否需要更换社区，如需要，则将更改的信息传播给邻居顶点
     * @param vertex 顶点
     * @param community 要与顶点所属社区比较的社区
     */
    public void changeCommunityIfNeed(Long vertex, Long community) {

        Long curCommunity = communityMap.get(vertex);
        if (community.equals(curCommunity))
            return;

        Long newCommunityNum = neighborCommunityMap.get(vertex).get(community);
        Long curCommunityNum = neighborCommunityMap.get(vertex).get(curCommunity);
        if (newCommunityNum > curCommunityNum || newCommunityNum == curCommunityNum && community < curCommunity) {
            communityMap.put(vertex, community);
            spread(vertex, curCommunity, community);
        }
    }

    /**
     * 将社区更新的消息传播给邻居顶点
     * @param vertex 顶点
     * @param oriCommunity
     * @param curCommunity
     */
    public void spread(Long vertex, Long oriCommunity, Long curCommunity) {
        Set<Long> neis = neighbors.get(vertex);
        updateCommunityNumber(vertex, oriCommunity, curCommunity);
        for (Long nei : neis) {
            updateCommunityNumber(nei, oriCommunity, curCommunity);
            changeCommunityIfNeed(nei, curCommunity);
        }
    }

    /**
     * 将不同窗口的图数据进行整合，然后对于可能会产生社区更新的顶点，判断、更新然后传播
     * @param otherLp
     */
    public void merge(LabelPropagationGlobalInfo otherLp) {

        Set<Tuple3<Long, Long, Long>> needToSpread = new HashSet<>();

        for (Long otherVertex : otherLp.communityMap.keySet()) {
            Long otherCommunity = otherLp.communityMap.get(otherVertex);

            if (!communityMap.containsKey(otherVertex)) {
                communityMap.put(otherVertex, otherCommunity);
                neighborCommunityMap.put(otherVertex, otherLp.neighborCommunityMap.get(otherVertex));
            } else {
                Map<Long, Long> thisMap = neighborCommunityMap.get(otherVertex);
                Map<Long, Long> otherMap = otherLp.neighborCommunityMap.get(otherVertex);
                Long community = communityMap.get(otherVertex);
                Long num = neighborCommunityMap.get(otherVertex).get(community);
                for (Long com : otherMap.keySet()) {
                    if (!thisMap.containsKey(com))
                        thisMap.put(com, otherMap.get(com));
                    else {
                        Long max = Math.max(thisMap.get(com), otherMap.get(com));
                        thisMap.put(com, max);
                    }
                }
                neighborCommunityMap.put(otherVertex, thisMap);

                Long otherNum = otherLp.neighborCommunityMap.get(otherVertex).get(otherCommunity);
                if (num < otherNum || num == otherNum && otherCommunity < community) {
                    communityMap.put(otherVertex, otherCommunity);
                    needToSpread.add(new Tuple3<>(otherVertex, community, otherCommunity));
                }
            }
        }

        for (Long otherVertex : otherLp.neighbors.keySet()) {
            if (!neighbors.containsKey(otherVertex))
                neighbors.put(otherVertex, new HashSet<Long>());
            neighbors.get(otherVertex).addAll(otherLp.neighbors.get(otherVertex));
            for (Long nei : otherLp.neighbors.get(otherVertex)) {
                if (!neighbors.containsKey(nei))
                    neighbors.put(nei, new HashSet<Long>());
                addEdge(otherVertex, nei);
            }
        }

        for (Tuple3<Long, Long, Long> tuple : needToSpread) {
            spread(tuple.f0, tuple.f1, tuple.f2);
        }
    }

    @Override
    public String toString() {

        StringBuilder res = new StringBuilder("");

        for (Long key : communityMap.keySet()) {
            res.append("(" + key + ", " + communityMap.get(key) + ")\n");
        }

        return res.toString();
    }
}

