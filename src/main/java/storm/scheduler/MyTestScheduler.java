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

        //获得拓扑的所有信息，確定Topology是否提交到集群了 topologyDetails不为null说明已经提交
        TopologyDetails topology = topologies.getByName("word_count");
        if (topology !=null){
            //topology是否需要进行调度分配,有可能之前分配过了
            boolean needScheduling = cluster.needsScheduling(topology);
//            long elapsedTime = System.currentTimeMillis();
//            System.out.println(elapsedTime);
            if (!needScheduling){
                System.out.println("The word_count topology DOES NOT NEED scheduling！");
            } else {
                System.out.println("The word_count topology DOES NEED scheduling！");
                //找出此拓扑的所有需求调度组件
                Map<String,List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                System.out.println("needs scheduling(component->executor)" + componentToExecutors);
                System.out.println("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                //SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName("MyTestScheduler"));
                //判断调度组件里是否含有需要调度的拓扑ID
                if (!componentToExecutors.containsKey("sentenceSpout")) {
                    System.out.println("The sentenceSpout topology DOES NOT NEED scheduling！");
                }else {
                    System.out.println("The sentenceSpout topology DOES NEED scheduling！");
                    //从调度组件中获取所需要执行属于拓扑ID的executors线程task
                    List<ExecutorDetails> executors = componentToExecutors.get("sentenceSpout");
                    //获取所有属于拓扑ID的supervisor信息
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails specialSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();
                        //从supervisor meta中找到和special-supervisorID对应的supervisor
                        if (meta.get("name").equals("special-supervisor")) {
                            specialSupervisor = supervisor;
                            break;
                        } else {
                            System.out.println("special-supervisor no exist!");
                        }
                    }

                    if (specialSupervisor!=null){
                        System.out.println("Found the special-supervisor");
                        //获取每个supervisor可用的slots列表
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        //获取每个supervisor已用的slots列表
                        Collection<WorkerSlot> usedsolts = cluster.getUsedSlots();
                        //如果在对应的supervisor没有可用的slots，选择释放一些slots
                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
                            }
                        }
                        //重新设置availableSlots
                        availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        //分配tasks到slots上
                        cluster.assign(availableSlots.get(0),topology.getId(),executors);
                        System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");

                    }else {
                        System.out.println("There is no supervisor named special-supervisor!!!");
                    }
                    //储存supervisor的name
//                    List<String> supervisorList = new ArrayList<String>();

                }
            }

        }
    //其余的任务由系统自带的EvenScheduler调度器执行
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
        map.put("sex","男");
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
        System.out.println("执行时间："+Time+"毫秒"+Time/(double)1000+"秒");

    }


}
