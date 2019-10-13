package pers.yue.performance.util;

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
}
