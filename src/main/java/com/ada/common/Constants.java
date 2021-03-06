package com.ada.common;

import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.globalTree.GTree;

import java.io.FileInputStream;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignKeyToParallelOperator;

public class Constants implements Serializable {

    /**
     * 定义Double类型的零
     */
    public final static double zero = 0.00001;

    public final static DecimalFormat df = new DecimalFormat("#.00000");

    public static int inputPartition;

    public static int densityPartition;

    /**
     * 全局索引的并行度
     */
    public static int globalPartition;

    /**
     * 本地索引的并行度
     */
    public static int dividePartition;


    public static int keyTIDPartition;

    /**
     * subTask: globalSubTask
     * value: key
     */
    public static Map<Integer,Integer> densitySubTaskKeyMap = new HashMap<>();

    /**
     * subTask: globalSubTask
     * value: key
     */
    public static Map<Integer,Integer> globalSubTaskKeyMap = new HashMap<>();

    /**
     * subTask: divideSubTask
     * value: key
     */
    public static Map<Integer,Integer> divideSubTaskKeyMap = new HashMap<>();

    /**
     * 密度统计的频度
     */
    public static int densityFre;

    /**
     * 网格密度
     */
    public final static int gridDensity = 511;

    public static int topK;

    public static int t;

    public static int KNum;

    public static double extend;

    public static long windowSize;

    public static int logicWindow;

    public static Rectangle globalRegion = new Rectangle(new Point(0.0,0.0), new Point(8626.0,8872.0));

    public static double extendToEnoughBig = (globalRegion.high.data[0] - globalRegion.low.data[0])*1.5;

    static {
        try {
            Properties pro = new Properties();
            String confPath;
            if ("Windows 10".equals(System.getProperty("os.name"))){
                confPath = "conf.properties";
            }else {
                confPath = "/home/chenliang/data/zzl/conf.properties";
            }
            FileInputStream in = new FileInputStream(confPath);
            pro.load(in);
            in.close();

            inputPartition = Integer.parseInt(pro.getProperty("inputPartition"));
            densityPartition = Integer.parseInt(pro.getProperty("densityPartition"));
            if (densityPartition <= 0 || (densityPartition & (densityPartition - 1)) != 0)
                throw new IllegalArgumentException();
            globalPartition = Integer.parseInt(pro.getProperty("globalPartition"));
            if (globalPartition <= 0 || (globalPartition & (globalPartition - 1)) != 0)
                throw new IllegalArgumentException();
            dividePartition = Integer.parseInt(pro.getProperty("dividePartition"));
//            if (dividePartition <= 0 || (dividePartition & (dividePartition - 1)) != 0)
//                throw new IllegalArgumentException();
            keyTIDPartition = Integer.parseInt(pro.getProperty("keyTIDPartition"));
            densityFre = Integer.parseInt(pro.getProperty("densityFre"));
            GTree.globalLowBound = Integer.parseInt(pro.getProperty("globalLowBound"));
            windowSize = Integer.parseInt(pro.getProperty("windowSize"));
            logicWindow = Integer.parseInt(pro.getProperty("logicWindow"));
            topK = Integer.parseInt(pro.getProperty("topK"));
            KNum = Integer.parseInt(pro.getProperty("KNum"));
            extend = Double.parseDouble(pro.getProperty("extend"));
            t = Integer.parseInt(pro.getProperty("t"));
        }catch (Exception e){
            e.printStackTrace();
        }

        /*
         * 86-- 128是 256
         */
        int maxParallelism;
        if (globalPartition < 86)
            maxParallelism= 128;
        else
            maxParallelism = 256;
        Set<Integer> usedSubTask = new HashSet<>();
        for (int i = 0; i < 1000000; i++) {
            Integer subTask = assignKeyToParallelOperator(i, maxParallelism, globalPartition);
            if (!usedSubTask.contains(subTask)) {
                usedSubTask.add(subTask);
                globalSubTaskKeyMap.put(subTask, i);
                if (usedSubTask.size() == globalPartition)
                    break;
            }
        }
        usedSubTask.clear();

        if (densityPartition < 86)
            maxParallelism= 128;
        else
            maxParallelism = 256;
        for (int i = 0; i < 1000000; i++) {
            Integer subTask = assignKeyToParallelOperator(i, maxParallelism, densityPartition);
            if (!usedSubTask.contains(subTask)) {
                usedSubTask.add(subTask);
                densitySubTaskKeyMap.put(subTask, i);
                if (usedSubTask.size() == densityPartition)
                    break;
            }
        }
        usedSubTask.clear();


        if (dividePartition < 86)
            maxParallelism= 128;
        else
            maxParallelism = 256;
        for (int i = 0; i < 1000000; i++) {
            Integer subTask = assignKeyToParallelOperator(i, maxParallelism, dividePartition);
            if (!usedSubTask.contains(subTask)) {
                usedSubTask.add(subTask);
                divideSubTaskKeyMap.put(subTask, i);
                if (usedSubTask.size() == dividePartition)
                    break;
            }
        }
    }

    /**
     * 缩减轨迹ID集合的元素数到 Constants.k*Constants.c 大小
     * @param selectedTIDs 被缩减的集合
     */
    public static void cutTIDs(Set<Integer> selectedTIDs) {
        Random random = new Random(12306);
        while (selectedTIDs.size() > topK*KNum){
            for (Iterator<Integer> iterator = selectedTIDs.iterator(); iterator.hasNext();){
                iterator.next();
                if (selectedTIDs.size() > topK*KNum){
                    if (random.nextInt()%4 == 0)
                        iterator.remove();
                }else {
                    break;
                }
            }
        }
    }


    public static boolean isEqual(double a, double b){
        return Math.abs(a - b) < zero;
    }

    public static void rectangleToInts(Rectangle rectangle, int[] low, int[] high){
        low[0] = (int) Math.round(rectangle.low.data[0]*10000);
        low[1] = (int) Math.round(rectangle.low.data[1]*10000);
        high[0] = (int) Math.round(rectangle.high.data[0]*10000);
        high[1] = (int) Math.round(rectangle.high.data[1]*10000);

    }

    /**
     * 返回rect0边长缩小多少后，能不与rect1相交。
     */
    public static double countShrinkBound(Rectangle rect0, Rectangle rect1) {
        if (!rect0.isIntersection(rect1))
            throw new IllegalArgumentException("count Shrink Bound error.");
        int[] r0_low = new int[2];
        int[] r0_high = new int[2];
        int[] r1_low = new int[2];
        int[] r1_high = new int[2];
        rectangleToInts(rect0, r0_low, r0_high);
        rectangleToInts(rect1, r1_low, r1_high);
        List<Integer> ins = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ins.add(r1_high[i] - r0_low[i]);
            ins.add(r0_high[i] - r1_low[i]);
        }
        return Collections.min(ins)/10000.0;
    }

    /**
     * 返回rect0边长扩大多少后，能与rect1相交。
     */
    public static double countEnlargeBound(Rectangle rect0, Rectangle rect1) {
        if (rect0.isIntersection(rect1))
            throw new IllegalArgumentException("count Enlarge Bound error.");
        int[] r0_low = new int[2];
        int[] r0_high = new int[2];
        int[] r1_low = new int[2];
        int[] r1_high = new int[2];
        rectangleToInts(rect0, r0_low, r0_high);
        rectangleToInts(rect1, r1_low, r1_high);
        List<Integer> ins = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            if ( r0_low[i] > r1_high[i] )
                ins.add(r0_low[i] - r1_high[i]);
            if( r0_high[i] < r1_low[i] )
                ins.add( r1_low[i] - r0_high[i] );
        }
        return Collections.max(ins)/10000.0;
    }

    /**
     * 返回rect0边长扩大多少后，能超出rect1的包围。
     */
    public static double countEnlargeOutBound(Rectangle rect0, Rectangle rect1) {
        if (!rect1.isInternal(rect0))
            throw new IllegalArgumentException("count Enlarge Bound error.");
        int[] r0_low = new int[2];
        int[] r0_high = new int[2];
        int[] r1_low = new int[2];
        int[] r1_high = new int[2];
        rectangleToInts(rect1, r0_low, r0_high);
        rectangleToInts(rect0, r1_low, r1_high);
        List<Integer> ins = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ins.add(r1_low[i] - r0_low[i]);
            ins.add( r0_high[i] - r1_high[i] );
        }
        return Collections.min(ins)/10000.0;
    }
}
