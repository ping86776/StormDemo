package storm.scheduler;

/**
 * Created by Ping on 2018/6/15.
 */

import java.util.*;
import java.util.logging.SimpleFormatter;

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
        logger.info("mytestscheduler");
        logger.info("+++++++++++++++++++++++++++");

        //������˵�������Ϣ���_��Topology�Ƿ��ύ����Ⱥ�� topologyDetails��Ϊnull˵���Ѿ��ύ
        TopologyDetails topology = topologies.getByName("word_count");
        if (topology !=null){
            //topology�Ƿ���Ҫ���е��ȷ���,�п���֮ǰ�������
            boolean needScheduling = cluster.needsScheduling(topology);
//            long elapsedTime = System.currentTimeMillis();
//            System.out.println(elapsedTime);
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
        final Random rand = new Random();
        long currentTime = System.currentTimeMillis();
//        int [] arr = new int[10000];
//        for (int i = 0; i < 10000; i++) {
//            arr[i]=rand.nextInt(10);
//            System.out.println(arr[i]);
//        }
//        HashSet executors = new HashSet();
//        for (int i = 0; i < 10000; i++) {
//            executors.add(arr[i]);
//        }
//        System.out.println(executors);
        HashMap<String,Object> map  = new HashMap<String, Object>();
        map.put("name","zhangsan");
        map.put("sex","��");
        map.put("age","22");
        map.put("high","170");
        System.out.println(map.values());
        List<String> list = new ArrayList<String>();
        for (String key : map.keySet()){
            list.add(key);
        }
//        System.out.println(list);
//        System.out.println(list.get(0));
        HashMap<String,Object> maps = new HashMap<String, Object>();
        HashMap<String,Object> map2 = new HashMap<String, Object>();
        for (String lists : list){
            String idx = (String)maps.get(lists);
            System.out.println(maps.get(lists));
            map2.put(idx,1);
        }
        list.get(1);
        System.out.println(map2);
        //System.out.println(map.keySet().getClass().toString());
        long elapsedTime = System.currentTimeMillis();
        long time = elapsedTime-currentTime;
        Double Time = Double.parseDouble(Long.toString(time));
        System.out.println("ִ��ʱ�䣺"+Time+"����"+Time/(double)1000+"��");

    }


}
