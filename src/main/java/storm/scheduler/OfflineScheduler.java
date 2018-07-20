package storm.scheduler;

import org.apache.log4j.Logger;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.resource.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Ping on 2018/7/17.
 */
public class OfflineScheduler {

    private Logger logger = Logger.getLogger(OfflineScheduler.class);
    //private AssignmentTracker assignmentTracker = new AssignmentTracker();

    public void schedule(Topologies topologies, Cluster cluster){
        logger.info("Offline Scheduler began start");
        logger.info("ping++++++++++++++++++++");

        try{
            for (TopologyDetails topology : topologies.getTopologies()) {
                //检查输出拓扑的名称和ID
                logger.debug("Checking topology " + topology.getName() + " (id: " + topology.getId() + ")");
                if (cluster.needsScheduling(topology)){
                    logger.debug("Topology " + topology.getId() + " needs rescheduling");
                    //获取所有component配置信息和名称
                    Map<String,Component> ComponentMap = topology.getComponents();
                    logger.info("ComponentMap+++++++++++++"+ComponentMap);
                    logger.info("ComponentMap+++++++++++++"+topology.getComponents());
                    List<String> componentList = new ArrayList<String>();
                    for (String Key : ComponentMap.keySet()){
                        componentList.add(Key);
                    }
                    logger.info("componentList+++++++++"+componentList);
//                    @SuppressWarnings("unchecked")List<String> componentList = (List<String>)topology.getComponents().keySet();
                    logger.info("values+++++++++"+topology.getComponents().values());
                    logger.info("keySet+++++++++"+topology.getComponents().keySet());
                    logger.info("Checking topology " + topology.getName() + " (id: " + topology.getId() + ")");
                    //@SuppressWarnings("unchecked")Map<String, List<String>> streamMap = (Map<String, List<String>>)topology.getConf().get("streams");
                    if (componentList !=null){
                        logger.info("components: " + Utils.collectionToString(componentList));

                        float a = 0;
                        if (topology.getConf().get("a") != null)
                            a = Float.parseFloat((String)topology.getConf().get("a"));
                        float b = 1;
                        if (topology.getConf().get("b") !=null)
                            b = Float.parseFloat((String)topology.getConf().get("b"));
                        float c = 0.5f ;
                        if (topology.getConf().get("c") !=null)
                            c =Float.parseFloat((String)topology.getConf().get("c"));
                        logger.info("a:" + a + ",b:" + b + ",c: " + c);

                        //准备 Slots
                        //获取拓扑的workers数量
                        logger.info("Number of workers++++++++++++++:" + topology.getNumWorkers());
                        List<List<ExecutorDetails>> slotList = new ArrayList<List<ExecutorDetails>>();
                        //根据拓扑配置的worker数来确定所需要的slot数，默认worker数=slot数
                        for (int i = 0; i < topology.getNumWorkers() ; i++) {
                            slotList.add(new ArrayList<ExecutorDetails>());
                        }
                        logger.info("slotList++++++++++++++"+slotList.size());
                        //计算有多少executors能被分配到single slot
                        //获取拓扑配置的所需的executor的数量
                        int executorCount = topology.getExecutors().size();
                        logger.info("executor+++++++++"+topology.getExecutors());
                        logger.info("executorCount+++++++++"+executorCount);
                        //求平均即每个slot平均能分配多少Executor
                        int min = (int)Math.ceil((double)executorCount/slotList.size());
                        int max = executorCount - slotList.size() + 1;
                        //每个Slot可分配executor的最大数量
                        int maxExecutorPerSlot = min + (int)Math.ceil(a * (max-min));
                        logger.info("Maximum number of executors per slot: " + maxExecutorPerSlot);

                        //component 已经分配的 executor 列表
                        //获得每个component对应的executors的映射关系 通过component的id找到对应executors的id
                        Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                        logger.info("componentToExecutors+++++++++++++"+componentToExecutors.get(1));

                        //logger.info("componentToExecutors+++++++++++++"+topology.getTopology());

                        //executor 分配好executor的Slots的索引
                        Map<ExecutorDetails,Integer> executorToSlotMap = new HashMap<ExecutorDetails, Integer>();

                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
                        logger.info("availableSlots+——+++++++"+availableSlots);

                        //遍历component
                        for (String component : componentList){
                            //logger.debug("Check for primary slots for component " + component);
                            logger.info("Check for primary slots for component " + component);
//                            List<String> inputComponentList = streamMap.get(component);
//                            logger.info("input components: " + Utils.collectionToString(inputComponentList));
                            //通过Key component的id获得value executor的id列表
                            List<ExecutorDetails> executorList = componentToExecutors.get(component);
                            logger.info("executors: " + Utils.collectionToString(executorList));

                            // identify primary slots and secondary slots
                            List<Integer> primarySlotList = new ArrayList<Integer>();
                            List<Integer> secondarySlotList = new ArrayList<Integer>();
                            //虚拟的，只是为了快速检查一个槽是否已经被使用。
                            Map<Integer, Integer> slotToUseMap = new HashMap<Integer, Integer>();
                            for (ExecutorDetails Executors : executorList){
                                for (int i = 0; i <maxExecutorPerSlot ; i++) {
                                    executorToSlotMap.put(Executors,i);
                                    logger.info("executorToSlotMap"+executorToSlotMap);
                                }
                            }
//                            if (inputComponentList != null) {
//                                // 对每个 input component, 跟踪当前分配相关的executor的Slot
//                                for (String inputComponent : inputComponentList) {
//                                    logger.debug("Checking input component " + inputComponent);
//                                    List<ExecutorDetails> inputExecutorList = componentToExecutors.get(inputComponent);
//                                    logger.debug("executors for input component " + inputComponent + ": " + Utils.collectionToString(inputExecutorList));
//                                    for (ExecutorDetails inputExecutor : inputExecutorList) {
//                                        int slotIdx = executorToSlotMap.get(inputExecutor);
//                                        slotToUseMap.put(slotIdx, 1);
//                                        logger.debug("input executor " + inputExecutor + " is assigned to slot " + slotIdx + ", so this slot is a primary one");
//                                    }
//                                }
//                            }
                            for (ExecutorDetails Executor : executorList){
                                int slotIdx = executorToSlotMap.get(Executor);
                                slotToUseMap.put(slotIdx,1);
                                logger.info("input executor " + Executor + " is assigned to slot " + slotIdx + ", so this slot is a primary one");
                            }


                            //如果slot包含输入component的executor，那么它就是主slot，否则就是二级slot
                            for (int i = 0; i < slotList.size(); i++) {
                                if (slotList.get(i).size() < maxExecutorPerSlot){
                                    if (slotToUseMap.get(i)!=null)//还有可用的slot
                                        primarySlotList.add(i);
                                    else
                                        secondarySlotList.add(i);
                                }

                            }

                            /*
							 * 如果二级插槽仍然为空，则将其提升为一级插槽
							 * 这样，我们确保所有的槽都被使用;
							 * 这是在已经安排了组件c之后完成的
							 */
                            logger.info("this component index: " + componentList.indexOf(component) + ", index of component where to start forcing to use empty slots: " + (int)(c * componentList.size()));
                            if (componentList.indexOf(component) >= (int)(c * componentList.size())) {
                                List<Integer> slotToPromoteList = new ArrayList<Integer>();
                                for (int secondarySlot : secondarySlotList)
                                    if (slotList.get(secondarySlot).isEmpty())
                                        slotToPromoteList.add(secondarySlot);
                                for (Integer slotToPromote : slotToPromoteList){
                                    secondarySlotList.remove(slotToPromote);
                                    primarySlotList.add(0,slotToPromote);
                                }
                            }

                            logger.info("Primary slots for component " + component + ": " + Utils.collectionToString(primarySlotList));
                            logger.info("Secondary slots for component " + component + ": " + Utils.collectionToString(secondarySlotList));

                            int primaryIdx = 0;
                            int secondaryIdx = 0;
                            for (ExecutorDetails executor : executorList){
                                logger.info("Assigning executor " + executor);
                                //以循环的方式为槽分配executor
                                //如果primarySlot可用(即为另一个executor提供足够的可用空间)，则将其分配给primarySlot
                                //否则就分配给secondary
                                int slotIdx = -1;//判断primary slot是否可用的标识符
                                //判断primarySlot是否可用
                                while (!primarySlotList.isEmpty() && slotList.get(primarySlotList.get(primaryIdx)).size() == maxExecutorPerSlot){
                                    logger.info("Primary slot " + primarySlotList.get(primaryIdx) + " is full, remove it");
                                    primarySlotList.remove(primaryIdx);
                                    if (primaryIdx == primarySlotList.size()){
                                        primaryIdx = 0;
                                        logger.info("Reached the tail of primary slot list, point to the head");
                                    }
                                }
                                if (!primarySlotList.isEmpty()){
                                    slotIdx = primarySlotList.get(primaryIdx);
                                    primaryIdx = (primaryIdx + 1) % primarySlotList.size();
                                    logger.info("primaryIdx++++"+primaryIdx);

                                }
                                //判断secondarySlot是否可用
                                if (slotIdx == -1){
                                    logger.info("No primary slot available, choose a secondary slot");
                                    while(!secondarySlotList.isEmpty() && slotList.get(secondarySlotList.get(secondaryIdx)).size() == maxExecutorPerSlot){
                                        logger.info("Secondary slot " + secondarySlotList.get(secondaryIdx) + " is full, remove it");
                                        secondarySlotList.remove(secondaryIdx);
                                        if (secondaryIdx == secondarySlotList.size()){
                                            secondaryIdx = 0;
                                            logger.info("Reached the tail of secondary slot list, point to the head");
                                        }
                                    }
                                    if (!secondarySlotList.isEmpty()){
                                        slotIdx = secondarySlotList.get(secondaryIdx);
                                        secondaryIdx = (secondaryIdx + 1)%secondarySlotList.size();

                                    }
                                }

                                if (slotIdx == -1)
                                    throw new Exception("Cannot assign executor " + executor + " to any slot");
                                slotList.get(slotIdx).add(executor);
                                logger.info("slotList111"+slotList.get(slotIdx).add(executor));
                                executorToSlotMap.put(executor,slotIdx);
                                logger.info("Assigned executor " + executor + " to slot " + slotIdx);
                                logger.info("executorToSlotMap  " + executorToSlotMap );
                            }


                        }/* end for (String component : componentList) */

                        // 计算要使用的node数量
                        //可用的slot
                        List<WorkerSlot> availableSlot = cluster.getAvailableSlots();
                        logger.info("availableSlots"+availableSlot);
                        NodeHelper nodeHelper = new NodeHelper(availableSlot,b,slotList.size());

                        // 以循环的方式，使用适当数量的node将executor分配给slot
                        int i = 0;
                        for (List<ExecutorDetails> executors : slotList) {
                            WorkerSlot worker = nodeHelper.getWorker(i);
                            cluster.assign(worker,topology.getId(),executors);
                            //将executor循环遍历进入slot<nodeId,portId>
                            logger.info("We assigned executors:" + Utils.collectionToString(executors) + " to slot: [" + worker.getNodeId() + ", " + worker.getPort() + "]");
                            i++;
                        }

                    }else {
                        logger.warn("No components and streams defined for topology " + topology);
                    }/* end if (components != null && streams != null) */

                }/* end if (cluster.needsScheduling(topology)) */

            }/* end for (TopologyDetails topology : topologies.getTopologies()) */

        } catch (Exception e){
            logger.error("An error occurred during the scheduling", e);
        }

        logger.info("ping---------------------------");
        logger.info("Calling EvenScheduler to schedule remaining executors...");
        new EvenScheduler().schedule(topologies, cluster);
        logger.info("Ok, EvenScheduler successfully called");
    }


    public void prepare(Map conf) {

    }
}
