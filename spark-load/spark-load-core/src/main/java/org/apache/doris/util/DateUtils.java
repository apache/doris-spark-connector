package org.apache.doris.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    public static final DateTimeFormatter NORMAL_FORMATER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(
                    ZoneId.systemDefault());

    public static final DateTimeFormatter NUMBER_FORMATER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(
                    ZoneId.systemDefault());

    public static String getFormattedNow(DateTimeFormatter formatter) {
        return formatter.format(LocalDateTime.now(ZoneId.systemDefault()));
    }

}
