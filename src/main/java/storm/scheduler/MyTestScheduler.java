package storm.scheduler;

/**
 * Created by Ping on 2018/6/15.
 */

import java.util.*;
import java.util.concurrent.Executor;

import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.Topologies;

import org.apache.log4j.Logger;

public class MyTestScheduler implements IScheduler{

    private Logger logger = Logger.getLogger(MyTestScheduler.class);
    private EvenScheduler evenScheduler = new EvenScheduler();

    private long lastRescheduling;

    public void prepare (Map conf){

    }

    public void schedule (Topologies topologies, Cluster cluster){

        System.out.println("MyTestScheduler: begin scheduling");
        //logger.info("MyTestScheduler");
        //logger.info("+++++++++++++++++++++++++++");

        //������˵�������Ϣ���_��Topology�Ƿ��ύ����Ⱥ�� topologyDetails��Ϊnull˵���Ѿ��ύ
        TopologyDetails topology = topologies.getByName("word_count");
        if (topology !=null){
            //topology�Ƿ���Ҫ���е��ȷ���,�п���֮ǰ�������
            boolean needScheduling = cluster.needsScheduling(topology);
            if (!needScheduling){
                System.out.println("The word_count topology DOES NOT NEED scheduling��");
            } else {
                System.out.println("The word_count topology DOES NEED scheduling��");
                //�ҳ������˵���������������
                Map<String,List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                System.out.println("needs scheduling(component->executor)" + componentToExecutors);
                System.out.println("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                //SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName("MyTestScheduler"));
                //�жϵ���������Ƿ�����Ҫ���ȵ�����ID
                if (!componentToExecutors.containsKey("sentenceSpout")) {
                    System.out.println("The sentenceSpout topology DOES NOT NEED scheduling��");
                }else {
                    System.out.println("The sentenceSpout topology DOES NEED scheduling��");
                    //�ӵ�������л�ȡ����Ҫִ����������ID��executors�߳�task
                    List<ExecutorDetails> executors = componentToExecutors.get("sentenceSpout");
                    //��ȡ������������ID��supervisor��Ϣ
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails specialSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();
                        //��supervisor meta���ҵ���special-supervisorID��Ӧ��supervisor
                        if (meta.get("name").equals("special-supervisor")) {
                            specialSupervisor = supervisor;
                            break;
                        } else {
                            System.out.println("special-supervisor no exist!");
                        }
                    }

                    if (specialSupervisor!=null){
                        System.out.println("Found the special-supervisor");
                        //��ȡÿ��supervisor���õ�slots�б�
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        //��ȡÿ��supervisor���õ�slots�б�
                        Collection<WorkerSlot> usedsolts = cluster.getUsedSlots();
                        //����ڶ�Ӧ��supervisorû�п��õ�slots��ѡ���ͷ�һЩslots
                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
                            }
                        }
                        //��������availableSlots
                        availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        //����tasks��slots��
                        cluster.assign(availableSlots.get(0),topology.getId(),executors);
                        System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");

                    }else {
                        System.out.println("There is no supervisor named special-supervisor!!!");
                    }
                    //����supervisor��name
//                    List<String> supervisorList = new ArrayList<String>();

                }
            }

        }
    //�����������ϵͳ�Դ���EvenScheduler������ִ��
    new EvenScheduler().schedule(topologies,cluster);
    int workers = topology.getNumWorkers();


    }

    public static void main(String[] args) {

        //List<String> componentList = (List<String>) topology.getConf().get("components");
        int[] arr = {2,5,3,4,5,6};
        HashSet executors = new HashSet();
        for (int i = 0; i < arr.length; i++) {
            //System.out.println(arr[i]);
//            if(!executors.add(arr[i])){
//                System.out.println(arr[i]);
//            }
            executors.add(arr[i]);
        }
        System.out.println(executors);

//        for (int i = 0; i < arr ; i++) {
//
//        }
    }


}