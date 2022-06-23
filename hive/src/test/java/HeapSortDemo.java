/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-06-23 15:54
 * @Version: 1.0
 * @Company: 58集团
 */

import java.util.Arrays;

public class HeapSortDemo {
    //默认升序
    private boolean asc = true;

    private int[] arr;

    public HeapSortDemo(int[] arr) {
        this.arr = arr;
    }

    public void sort(boolean asc) {
        this.asc = asc;
        this.sort();
    }

    /**
     * 堆排序的主要入口方法，共两步。
     */
    public void sort() {
        /*
         *  第一步：将数组堆化（可大堆或小堆）
         *  beginIndex = 第一个非叶子节点。
         *  从第一个非叶子节点开始即可。无需从最后一个叶子节点开始。
         *  叶子节点可以看作已符合堆要求的节点，根节点就是它自己且自己以下值为最大。
         */
        int len = this.arr.length - 1; //最后元素下标
        //右移一位就是除二操作，最后一个父节点下标(length/2 - 1)来源于(父:i>1 左子:2i 右子:2i+1)，最后一个节点length，故 i-1 = length/2 -1
        int beginIndex = (len - 1) >> 1; //(length -1 -1)/2
        for (int i = beginIndex; i >= 0; i--) {
            maxHeapify(i, len);
        }

        /*
         * 第二步：对堆化数据排序(第一步只是得到了最大或最小推)
         * 每次都是移出最顶层的根节点A[0]，与最尾部节点位置调换，同时遍历长度 - 1。
         * 然后从新整理被换到根节点的末尾元素，使其符合堆的特性。
         * 直至未排序的堆长度为 0。
         */
        for (int i = len; i > 0; i--) {
            swap(0, i);
            maxHeapify(0, i - 1);
        }
    }

    private void swap(int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    /**
     * 调整索引为 index 处的数据，使其符合堆的特性。
     *
     * @param index 需要堆化处理的数据的索引
     * @param len   未排序的堆（数组）的长度
     */
    private void maxHeapify(int index, int len) {
        int li = (index << 1) + 1; // 左子节点索引
        int ri = li + 1;           // 右子节点索引
        int cMax = li;             // 子节点值最大索引，默认左子节点。

        if (li > len) return;       // 左子节点索引超出计算范围，直接返回。
        if (this.asc) { //升序用大堆
            if (ri <= len && arr[ri] > arr[li]) // 先判断左右子节点，哪个较大。
                cMax = ri;
            if (arr[cMax] > arr[index]) {
                swap(cMax, index);      // 如果父节点被子节点调换，
                maxHeapify(cMax, len);  // 则需要继续判断换下后的父节点是否符合堆的特性。
            }
        } else { //降序用小堆
            if (ri <= len && arr[ri] < arr[li]) // 先判断左右子节点，哪个较小。
                cMax = ri;
            if (arr[cMax] < arr[index]) {
                swap(cMax, index);      // 如果父节点被子节点调换，
                maxHeapify(cMax, len);  // 则需要继续判断换下后的父节点是否符合堆的特性。
            }
        }
    }

    /**
     * 测试用例:
     * int[]{3, 5, 3, 0, 8, 6, 1, 5, 8, 6, 2, 4, 9, 4, 7, 0, 1, 8, 9, 7, 3, 1, 2, 5, 9, 7, 4, 0, 2, 6}
     * 输出:
     * [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9]
     * [9, 9, 9, 8, 8, 8, 7, 7, 7, 6, 6, 6, 5, 5, 5, 4, 4, 4, 3, 3, 3, 2, 2, 2, 1, 1, 1, 0, 0, 0]
     */
    public static void main(String[] args) {
        int[] arr = new int[]{3, 5, 3, 0, 8, 6, 1, 5, 8, 6, 2, 4, 9, 4, 7, 0, 1, 8, 9, 7, 3, 1, 2, 5, 9, 7, 4, 0, 2, 6};
        new HeapSortDemo(arr).sort(false);
        System.out.println(Arrays.toString(arr));
        //除二向下取整
        //System.out.println((14 - 1) >> 1);
        //System.out.println((15 - 1) >> 1);
    }
}
