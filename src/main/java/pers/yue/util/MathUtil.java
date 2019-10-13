package pers.yue.util;

import java.text.DecimalFormat;

public class MathUtil {

    public static float round(float number, int numDecimalDigits) {
        StringBuilder sb = new StringBuilder(".");
        for(int i = 0; i < numDecimalDigits; i++) {
            sb.append("0");
        }
        DecimalFormat decimalFormat=new DecimalFormat(sb.toString());
        String numString = decimalFormat.format(number);
        return Float.valueOf(numString);
    }

    public static class Fibonacci {
        private int n1 = 0, n2 = 1;

        public Fibonacci skip(int numSkip) {
            for(int i = 0; i < numSkip; i++) {
                getInt();
            }
            return this;
        }

        public Fibonacci reset() {
            n1 = 0;
            n2 = 1;
            return this;
        }

        public int getInt() {
            int result = n1;
            int sumOfPrevTwo = n1 + n2;
            n1 = n2;
            n2 = sumOfPrevTwo;
            return result;
        }
    }
}
