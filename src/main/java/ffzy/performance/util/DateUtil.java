package ffzy.performance.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by zhangyue58 on 2018/01/11
 */
public class DateUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static String FORMAT_SIMPLE = "yyyy-MM-dd HH:mm:ss";
    public static String FORMAT_LONG = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static String FORMAT_Z = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static String FORMAT_SHORT_COMPACT = "yyyyMMdd-HHmmss.SSS";
    public static String FORMAT_EXPIRES_DATE = "EEE MMM dd zzz HH:mm:ss yyyy";
    public static String FORMAT_DATE = "EEE, dd MMM yyyy HH:mm:ss z";

    public static long getCurrentTime() {
        return Calendar.getInstance().getTimeInMillis();
    }

    public static long getSpecifiedTime(int pastInSecond) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, pastInSecond);
        return calendar.getTimeInMillis();
    }

    public static String formatTime(long unixTime, String format) {
        return formatTime(unixTime, format, Locale.US);
    }

    public static String formatTime(long unixTime, String format, Locale locale) {
        SimpleDateFormat sdf = new SimpleDateFormat(format, locale);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.format(unixTime);
    }

    public static String formatTime(long unixTime, String format, TimeZone timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.US);
        sdf.setTimeZone(timeZone);
        return sdf.format(unixTime);
    }

    public static long unformatTime(String formattedTime, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            return sdf.parse(formattedTime).getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    public static boolean isDateClosed(Date date) {
        return isDateClosed(date, 1000 * 60 * 15);
    }

    public static boolean isDateClosed(Date date, long interval){
        Date now = new Date();
        long nowTime = now.getTime();
        long dateTime = date.getTime();
        if (interval < 0) {
            interval = -interval;
        }
        boolean ret = (nowTime - dateTime < interval) && (dateTime - nowTime < interval);
        if(!ret) {
            logger.info(String.format("Date is out of expected scope. actual time:%d, expected time:%d, scope range:%d",
                    dateTime, nowTime, interval));
        }
        return ret;
    }
}
