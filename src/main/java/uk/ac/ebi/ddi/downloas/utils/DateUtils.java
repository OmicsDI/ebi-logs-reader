package uk.ac.ebi.ddi.downloas.utils;

import org.elasticsearch.common.collect.Tuple;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;

public class DateUtils {

    private DateUtils() {
    }

    public static Date atStartOfDay(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        LocalDateTime startOfDay = localDateTime.with(LocalTime.MIN);
        return localDateTimeToDate(startOfDay);
    }

    public static Date atEndOfDay(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        LocalDateTime endOfDay = localDateTime.with(LocalTime.MAX);
        return localDateTimeToDate(endOfDay);
    }

    private static LocalDateTime dateToLocalDateTime(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    private static Date localDateTimeToDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static List<Tuple<Date, Date>> partition(Date from, Date to, int size) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(from);

        Calendar endCalendar = new GregorianCalendar();
        endCalendar.setTime(to);

        List<Tuple<Date, Date>> result = new ArrayList<>();

        while (calendar.before(endCalendar)) {
            Date firstDate = calendar.getTime();
            calendar.add(Calendar.DATE, size);
            Date endDate = atEndOfDay(calendar.getTime());
            if (endDate.after(to)) {
                result.add(new Tuple<>(firstDate, to));
            } else {
                result.add(new Tuple<>(firstDate, endDate));
            }
            calendar.add(Calendar.DATE, 1);
        }
        return result;
    }
}
