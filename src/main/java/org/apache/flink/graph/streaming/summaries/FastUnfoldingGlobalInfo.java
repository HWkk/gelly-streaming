package org.apache.flink.graph.streaming.summaries;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Created by k81023208 on 2017/7/25.
 */
public class FastUnfoldingGlobalInfo implements Serializable{

    private Map<Long, Long> communityMap;
    private Map<Long, CommunityValue> valueOfCommunity;
    private Map<Long, Set<Long>> neighbors;



}

class CommunityValue {

}