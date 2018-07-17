package storm.scheduler;

import org.apache.log4j.Logger;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Topologies;

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

    public void schdule(Topologies topologies, Cluster cluster){
        logger.info("Offline Scheduler began start");
        logger.info("ping++++++++++++++++++++");

        try{
            for (TopologyDetails topology : topologies.getTopologies()) {
                //���������˵����ƺ�ID
                logger.debug("Checking topology " + topology.getName() + " (id: " + topology.getId() + ")");
                if (cluster.needsScheduling(topology)){
                    logger.debug("Topology " + topology.getId() + " needs rescheduling");
                    //��ȡ����component������Ϣ������
                    List<String> componentList = (List<String>)topology.getConf().get("component");
                    Map<String, List<String>> streamMap = (Map<String, List<String>>)topology.getConf().get("streams");
                    if (componentList !=null){
                        logger.debug("components: " + Utils.collectionToString(componentList));

                        float a = 0;
                        if (topology.getConf().get("a") != null)
                            a = Float.parseFloat((String)topology.getConf().get("a"));
                        float b = 1;
                        if (topology.getConf().get("b") !=null)
                            b = Float.parseFloat((String)topology.getConf().get("b"));
                        float c = 0.5f ;
                        if (topology.getConf().get("c") !=null)
                            c =Float.parseFloat((String)topology.getConf().get("c"));
                        logger.debug("a:" + a + ",b:" + b + ",c: " + c);

                        //׼�� Slots
                        //��ȡ���˵�workers����
                        logger.debug("Number of workers:" + topology.getNumWorkers());
                        List<List<ExecutorDetails>> slotList = new ArrayList<List<ExecutorDetails>>();
                        for (int i = 0; i < topology.getNumWorkers() ; i++) {
                            slotList.add(new ArrayList<ExecutorDetails>());
                        }

                        //�����ж���executors�ܱ����䵽single slot
                        //��ȡ�������õ������executor������
                        int executorCount = topology.getExecutors().size();
                        int min = (int)Math.ceil((double)executorCount/slotList.size());
                        int max = executorCount - slotList.size() + 1;
                        //ÿ��Slot�ɷ���executor���������
                        int maxExecutorPerSlot = min + (int)Math.ceil(a * (max-min));
                        logger.debug("Maximum number of executors per slot: " + maxExecutorPerSlot);

                        //component �Ѿ������ executor �б�
                        //���ÿ��component��Ӧ��executors��ӳ���ϵ ͨ��component��id�ҵ���Ӧexecutors��id
                        Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

                        //executor �����executor��Slots������
                        Map<ExecutorDetails,Integer> executorToSlotMap = new HashMap<ExecutorDetails, Integer>();

                        //����component
                        for (String component : componentList){
                            //logger.debug("Check for primary slots for component " + component);
                            logger.debug("Check for primary slots for component " + component);
                            List<String> inputComponentList = streamMap.get(component);
                            logger.debug("input components: " + Utils.collectionToString(inputComponentList));
                            //ͨ��component���executor��id�б�
                            List<ExecutorDetails> executorList = componentToExecutors.get(component);
                            logger.debug("executors: " + Utils.collectionToString(executorList));

                            // identify primary slots and secondary slots
                            List<Integer> primarySlotList = new ArrayList<Integer>();
                            List<Integer> secondarySlotList = new ArrayList<Integer>();
                            //����ģ�ֻ��Ϊ�˿��ټ��һ�����Ƿ��Ѿ���ʹ�á�
                            Map<Integer, Integer> slotToUseMap = new HashMap<Integer, Integer>();
                            if (inputComponentList != null) {
                                // ��ÿ�� input component, ���ٵ�ǰ������ص�executor��Slot
                                for (String inputComponent : inputComponentList) {
                                    logger.debug("Checking input component " + inputComponent);
                                    List<ExecutorDetails> inputExecutorList = componentToExecutors.get(inputComponent);
                                    logger.debug("executors for input component " + inputComponent + ": " + Utils.collectionToString(inputExecutorList));
                                    for (ExecutorDetails inputExecutor : inputExecutorList) {
                                        int slotIdx = executorToSlotMap.get(inputExecutor);
                                        slotToUseMap.put(slotIdx, 1);
                                        logger.debug("input executor " + inputExecutor + " is assigned to slot " + slotIdx + ", so this slot is a primary one");
                                    }
                                }
                            }

                            //���slot��������component��executor����ô��������slot��������Ƕ���slot
                            for (int i = 0; i < slotList.size(); i++) {
                                if (slotList.get(i).size() < maxExecutorPerSlot){
                                    if (slotToUseMap.get(i)!=null)//���п��õ�slot
                                        primarySlotList.add(i);
                                    else
                                        secondarySlotList.add(i);
                                }

                            }

                            /*
							 * ������������ȻΪ�գ���������Ϊһ�����
							 * ����������ȷ�����еĲ۶���ʹ��;
							 * �������Ѿ����������c֮����ɵ�
							 */
                            logger.debug("this component index: " + componentList.indexOf(component) + ", index of component where to start forcing to use empty slots: " + (int)(c * componentList.size()));
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

                            logger.debug("Primary slots for component " + component + ": " + Utils.collectionToString(primarySlotList));
                            logger.debug("Secondary slots for component " + component + ": " + Utils.collectionToString(secondarySlotList));

                            int primaryIdx = 0;
                            int secondaryIdx = 0;
                            for (ExecutorDetails executor : executorList){
                                logger.debug("Assigning executor " + executor);
                                //��ѭ���ķ�ʽΪ�۷���executor
                                //���primarySlot����(��Ϊ��һ��executor�ṩ�㹻�Ŀ��ÿռ�)����������primarySlot
                                //����ͷ����secondary
                                int slotIdx = -1;//�ж�primary slot�Ƿ���õı�ʶ��
                                //�ж�primarySlot�Ƿ����
                                while (!primarySlotList.isEmpty() && slotList.get(primarySlotList.get(primaryIdx)).size() == maxExecutorPerSlot){
                                    logger.debug("Primary slot " + primarySlotList.get(primaryIdx) + " is full, remove it");
                                    primarySlotList.remove(primaryIdx);
                                    if (primaryIdx == primarySlotList.size()){
                                        primaryIdx = 0;
                                        logger.debug("Reached the tail of primary slot list, point to the head");
                                    }
                                }
                                if (!primarySlotList.isEmpty()){
                                    slotIdx = primarySlotList.get(primaryIdx);
                                    primaryIdx = (primaryIdx + 1) % primarySlotList.size();

                                }
                                //�ж�secondarySlot�Ƿ����
                                if (slotIdx == -1){
                                    logger.debug("No primary slot availble, choose a secondary slot");
                                    while(!secondarySlotList.isEmpty() && slotList.get(secondarySlotList.get(secondaryIdx)).size() == maxExecutorPerSlot){
                                        logger.debug("Secondary slot " + secondarySlotList.get(secondaryIdx) + " is full, remove it");
                                        secondarySlotList.remove(secondaryIdx);
                                        if (secondaryIdx == secondarySlotList.size()){
                                            secondaryIdx = 0;
                                            logger.debug("Reached the tail of secondary slot list, point to the head");
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
                                executorToSlotMap.put(executor,slotIdx);
                                logger.debug("Assigned executor " + executor + " to slot " + slotIdx);
                            }


                        }/* end for (String component : componentList) */

                        // ����Ҫʹ�õ�node����
                        //���õ�slot
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
                        NodeHelper nodeHelper = new NodeHelper(availableSlots,b,slotList.size());

                        // ��ѭ���ķ�ʽ��ʹ���ʵ�������node��executor�����slot
                        int i = 0;
                        for (List<ExecutorDetails> slot : slotList) {
                            WorkerSlot worker = nodeHelper.getWorker(i);
                            cluster.assign(worker,topology.getId(),slot);
                            //��executorѭ����������slot<nodeId,portId>
                            logger.info("We assigned executors:" + Utils.collectionToString(slot) + " to slot: [" + worker.getNodeId() + ", " + worker.getPort() + "]");
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
