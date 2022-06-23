package com.han.bigdata.hive.demo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-14 20:07
 * @Version: 1.0
 * @Company: 58集团
 */
public class ListDemo {
    public static void main(String[] args) {
        ArrayList<Double> list = new ArrayList<>();
        ArrayList<Object> arrayList = new ArrayList<Object>();
        arrayList.add(1);
        arrayList.add(4);
        arrayList.add(3);
        arrayList.add(2);

        arrayList.forEach(
                new Consumer<Object>() {
                    @Override
                    public void accept(Object aDouble) {
                        list.add(Double.valueOf(aDouble.toString()));
                    }
                }
        );
        System.out.println(list);
        System.out.println("------------------------------------------");
        Iterator<Object> iterator = arrayList.iterator();
        while (iterator.hasNext()) {
            Object aDouble =  iterator.next();
            list.add(Double.valueOf(aDouble.toString()));
        }
        System.out.println(list);
        System.out.println("------------------------------------------");
        for (Object aDouble : arrayList) {
            list.add(Double.valueOf(aDouble.toString()));
        }
        System.out.println(list);
    }
}
