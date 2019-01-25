package uk.ac.ebi.ddi.downloas.utils;

import org.elasticsearch.common.collect.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DateUtilsTest {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Test
    public void paritition() throws ParseException {
        Date fromDate = dateFormat.parse("2018-12-20");
        Date toDate = dateFormat.parse("2019-01-23");

        List<Tuple<Date, Date>> result = DateUtils.partition(fromDate, toDate, 30);
        Assert.assertEquals(2, result.size());
    }
}