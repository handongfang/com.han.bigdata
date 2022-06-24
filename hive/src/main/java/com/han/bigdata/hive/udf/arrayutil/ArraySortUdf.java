package com.han.bigdata.hive.udf.arrayutil;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-07 11:44
 * @Version: 1.0
 * @Company: 58集团
 */
public class ArraySortUdf extends UDF {
    private Logger log = LoggerFactory.getLogger(ArraySortUdf.class.getName());
    private String SORTWAY = "ASC"; //默认升序
    private boolean PURENUMBERS = false; //默认非纯数字
    private boolean ISINTEGER = false; //默认非整形
    private Object arrayList = null;

    public ArraySortUdf() {
        this.SORTWAY = "ASC";
    }

    /**
     * 全采用默认值，ACS、字符串、非整形
     *
     * @param hiveArray
     * @return
     * @throws Exception
     */
    public List<String> evaluate(Object hiveArray) throws Exception {
        return evaluate(hiveArray, SORTWAY, PURENUMBERS, ISINTEGER);
    }

    /**
     * 指定ASC或DESC，默认是字符串、非整形
     *
     * @param hiveArray
     * @param sortWay
     * @return
     * @throws Exception
     */
    public List<String> evaluate(Object hiveArray, String sortWay) throws Exception {
        return evaluate(hiveArray, sortWay, PURENUMBERS, ISINTEGER);
    }

    /**
     * 指定是非数值，默认ASC与非整形
     *
     * @param hiveArray
     * @param pureNumbers
     * @return
     * @throws Exception
     */
    public List<String> evaluate(Object hiveArray, boolean pureNumbers) throws Exception {
        return evaluate(hiveArray, SORTWAY, pureNumbers, ISINTEGER);
    }

    /**
     * 指定ASC或DESC以及是非纯数值，默认非整形
     *
     * @param hiveArray
     * @param sortWay
     * @param pureNumbers
     * @return
     * @throws Exception
     */
    public List<String> evaluate(Object hiveArray, String sortWay, boolean pureNumbers) throws Exception {
        return evaluate(hiveArray, sortWay, pureNumbers, ISINTEGER);
    }

    /**
     * 指定是非数值与是非整形，默认ASC
     *
     * @param hiveArray
     * @param pureNumbers
     * @param isInteger
     * @return
     * @throws Exception
     */
    public List<String> evaluate(Object hiveArray, boolean pureNumbers, boolean isInteger) throws Exception {
        return evaluate(hiveArray, SORTWAY, pureNumbers, isInteger);
    }

    /**
     * 全自定义
     *
     * @param hiveArray
     * @param sortWay
     * @param pureNumbers
     * @param isInteger
     * @return
     * @throws Exception
     */
    public List<String> evaluate(Object hiveArray, String sortWay, boolean pureNumbers, boolean isInteger) throws Exception {
        try {
            this.arrayList = hiveArray;//
            if (!"ASC".equals(sortWay) && !"DESC".equals(sortWay)) {
                //错误抛出至外层捕捉
                throw new Exception(String.format(" - 你所输入的排序方式有误：降序（DESC）、升序（ASC）, 你的输入：%s", sortWay));
            } else if (hiveArray == null || !(hiveArray instanceof List)) {
                return new ArrayList<String>();
            } else if (((List<String>) hiveArray).size() == 1) {
                return (List<String>) hiveArray;
            } else if ("ASC".equals(sortWay)) {
                arraySort(pureNumbers, isInteger, Comparator.naturalOrder());
            } else if ("DESC".equals(sortWay)) {
                arraySort(pureNumbers, isInteger, Comparator.reverseOrder());
            }
            //最终返回字符串数组
            return (List<String>) this.arrayList;
        } catch (Exception e) {
            //错误抛出至外层捕捉
            throw new Exception(ArraySortUdf.class.getName() + "-" + e.getMessage());
        }
    }

    /**
     * 自定义排序
     *
     * @param pureNumbers
     * @param isInteger
     * @param c
     * @return
     */
    public void arraySort(boolean pureNumbers, boolean isInteger, Comparator c) {
        if (pureNumbers && !isInteger) {
            //纯数字默认为Double处理（但会改变原有数据类型）
            ArrayList<Double> list = new ArrayList<>();
            //
            for (Object strDouble : ((List<Object>) this.arrayList)) {
                list.add(Double.valueOf(strDouble.toString()));
            }
            //排序
            list.sort(c);
            this.arrayList = list;
        } else if (pureNumbers && isInteger) {
            //Integer排序
            ArrayList<Integer> list = new ArrayList<>();
            //
            for (Object strInteger : ((List<Object>) this.arrayList)) {
                list.add(Integer.valueOf(strInteger.toString()));
            }
            //排序
            list.sort(c);
            this.arrayList = list;
        } else {
            //String排序
            ArrayList<String> list = new ArrayList<>();
            //
            for (Object str : ((List<Object>) this.arrayList)) {
                list.add(str.toString());
            }
            //排序
            list.sort(c);
            this.arrayList = list;
        }
    }

    public static void main(String[] args) throws Exception {
        ArrayList<Object> arrayList = new ArrayList<Object>();
        arrayList.add(4);
        arrayList.add("10");
        arrayList.add(3);
        arrayList.add("2");
        System.out.println(arrayList);
        System.out.println("-------");
        ArraySortUdf arraySort = new ArraySortUdf();
        List<String> hiveArray = arraySort.evaluate(arrayList, false);
        System.out.println(hiveArray);
        System.out.println("-------");
        List<String> hiveArray2 = arraySort.evaluate(arrayList, "DESC", true, true);
        System.out.println(hiveArray2);
        System.out.println("-------");
        List<String> hiveArray5 = arraySort.evaluate(arrayList, "DESC", true);
        System.out.println(hiveArray5);
        System.out.println("-------");
        List<String> hiveArray4 = arraySort.evaluate(null);
        System.out.println(hiveArray4);
        System.out.println("-------");
        List<String> hiveArray6 = arraySort.evaluate("非数组返回[]", "ASC");
        System.out.println(hiveArray6);
        System.out.println("-------");
        List<String> hiveArray3 = arraySort.evaluate(arrayList, "DESC/ASC");
        System.out.println(hiveArray3);
    }
}
