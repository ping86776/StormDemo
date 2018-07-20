/**
 * Created by Ping on 2018/6/5.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Topologies;

import java.util.List;

public class MainTopology {
    private TopologyBuilder builder;
    private Config config;

    public MainTopology() {
        this.builder = new TopologyBuilder();
        //parallelism是设置Executor的数量
        this.builder.setSpout("sentenceSpout", new SentenceSpout(), 16);
        this.builder.setBolt("splitBolt", new SplitBolt(), 16).shuffleGrouping("sentenceSpout");
        this.builder.setBolt("countBolt", new CountBolt(), 16).shuffleGrouping("splitBolt");

        this.config = new Config();
    }
    //集群模式
    public void runCluster() {
        try {
            config.setNumWorkers(4);
            config.setMaxTaskParallelism(2);
            StormSubmitter.submitTopology("word_count",this.config,this.builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //本地模式
    public void runLocal(int waitSeconds) {
        LocalCluster cluster = new LocalCluster();
        config.setNumWorkers(4);
        config.setMaxTaskParallelism(2);
        config.get("component");
        cluster.submitTopology("word_count", this.config, this.builder.createTopology());

        try {
            Thread.sleep(waitSeconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology("word_count");
        cluster.shutdown();
    }

    public static void main(String[] args) {
        MainTopology topology = new MainTopology();
//        @SuppressWarnings("unchecked")List<String> componentList = (List<String>)topology.get("component");
        topology.runLocal(60);
        System.out.println("拓扑信息：++++++++++++++"+topology.builder);
        //topology.runCluster();
    }
}
