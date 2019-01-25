package uk.ac.ebi.ddi.downloas.utils;

import org.elasticsearch.common.collect.Tuple;

import java.util.*;

public class DateUtils {

    private DateUtils() {
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
            if (calendar.getTime().after(to)) {
                result.add(new Tuple<>(firstDate, to));
            } else {
                result.add(new Tuple<>(firstDate, calendar.getTime()));
            }
            calendar.add(Calendar.DATE, 1);
        }
        return result;
    }
}
