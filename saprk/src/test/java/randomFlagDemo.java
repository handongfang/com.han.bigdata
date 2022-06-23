import com.codahale.metrics.Gauge;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.util.Random;
import java.util.UUID;

public class randomFlagDemo {
    public static void main(String[] args) {
        UUID uuid = UUID.randomUUID();
        System.out.println(uuid);
        String rs = RandomStringUtils.randomAlphabetic(10);
        System.out.println(rs);
        int ri = RandomUtils.nextInt(5, 10);
        System.out.println(ri);
        /**
         * Random random = new Random(0L);
         * int ri = 5 + random.nextInt(10-5);
         */
        for (int i = 0; i < 10; i++) {
            int len = RandomUtils.nextInt(5, 10);
            String randomWord = RandomWord(len);
            System.out.printf("%s:%s %n", randomWord, len);
        }
    }

    public static String RandomWord(int len) {
        return RandomWord(len, len);
    }

    public static String RandomWord(int minLen, int maxLen) {
        return RandomWord(minLen, maxLen, 0L);
    }

    public static String RandomWord(int minLen, int maxLen, long seed) {
        StringBuilder stringBuilder = new StringBuilder();
        Random random = new Random(seed);
//        int len = minLen + ((maxLen == minLen) ? 0 : random.nextInt(maxLen - minLen));
//        while (len-- > 0) {
//            stringBuilder.append(RandomStringUtils.randomAlphabetic(1));
//        }
        stringBuilder.append(RandomStringUtils.randomAlphabetic(minLen, maxLen + 1));
        return stringBuilder.toString();
    }
}
