package org.apache.flink.graph.streaming.summaries;


import org.apache.flink.graph.Edge;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

public class SemiCluster implements Comparable<SemiCluster>, Serializable{

    double innerEdge;
    double boundaryEdge;
    double score;
    HashSet<Long> vertices;

    public SemiCluster(Long id, double boundaryEdge) {
        innerEdge = 0.0;
        this.boundaryEdge = boundaryEdge;
        score = 0.0;
        vertices = new HashSet<>();
        vertices.add(id);
    }

    public SemiCluster(SemiCluster ori) {
        innerEdge = ori.innerEdge;
        boundaryEdge = ori.boundaryEdge;
        score = ori.score;
        vertices = new HashSet<>();
        vertices.addAll(ori.vertices);
    }

    /**
     * 在当前cluster加入一个节点
     */
    public void addVertex(Long vertexId, HashMap<Long, Double> neighbors, double scoreFactor) {

        if(vertices.add(vertexId)) {
            for(Long neighbor : neighbors.keySet()) {
                if(vertices.contains(neighbor)) {
                    innerEdge += neighbors.get(neighbor);
                    boundaryEdge -= neighbors.get(neighbor);
                } else
                    boundaryEdge += neighbors.get(neighbor);
            }

            int size = vertices.size();

            score = (innerEdge - scoreFactor * boundaryEdge) / (size * (size - 1) / 2.0); //score的计算公式
        }
    }

    @Override
    public final int compareTo(final SemiCluster other) {
        if (other == null) {
            return 1;
        }
        if (this.vertices.size() < other.vertices.size()) {
            return -1;
        }
        if (this.vertices.size() > other.vertices.size()) {
            return 1;
        }
        if (other.vertices.containsAll(vertices)) {
            return 0;
        }
        return -1;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SemiCluster other = (SemiCluster) obj;
        if (vertices == null) {
            if (other.vertices != null) {
                return false;
            }
        } else if (!vertices.equals(other.vertices)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "SemiCluster{" +
                "score=" + score +
                ", vertices=" + vertices +
                '}';
    }

}
