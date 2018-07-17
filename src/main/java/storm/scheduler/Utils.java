package storm.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by Ping on 2018/7/16.
 */
public class Utils {

    // public static final int ACKER_TAKS_ID = 1;
    public static final String ALFA = "alfa"; // between 0 and 1
    public static final String BETA = "beta"; // between 0 and 1
    public static final String GAMMA = "gamma"; // greater than 1
    public static final String DELTA = "delta"; // between 0 and 1
    public static final String TRAFFIC_IMPROVEMENT = "traffic.improvement"; // between 1 and 100
    public static final String RESCHEDULE_TIMEOUT = "reschedule.timeout"; // in s


    private Utils() {}

    /**
     * @param list
     * @return the list in csv format
     */
    public static String collectionToString(Collection<?> list) {
        if (list == null)
            return "null";
        if (list.isEmpty())
            return "<empty list>";
        StringBuffer sb = new  StringBuffer();
        int i = 0;
        for (Object item : list) {
            sb.append(item.toString());
            if (i < list.size() - 1)
                sb.append(", ");
            i++;
        }
        return sb.toString();
    }

    private String mapToString(Map<String, List<String>> map) {
        if (map == null)
            return "null";
        if (map.keySet().isEmpty())
            return "<empty map>";
        StringBuffer sb = new  StringBuffer();
        int i = 0;
        for (Object key : map.keySet()) {
            sb.append(key.toString() + " -> (");
            sb.append(collectionToString(map.get(key)) + ")");
            if (i < map.keySet().size() - 1)
                sb.append(", ");
            i++;
        }
        return sb.toString();
    }
}
