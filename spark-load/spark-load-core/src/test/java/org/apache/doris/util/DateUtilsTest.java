package org.apache.doris.util;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

class DateUtilsTest {

    @Test
    void getFormattedNow() {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now(ZoneId zoneId) {
                return LocalDateTime.of(2024,8,1,12,34,56);
            }
        };
        Assertions.assertEquals("2024-08-01 12:34:56", DateUtils.getFormattedNow(DateUtils.NORMAL_FORMATER));
        Assertions.assertEquals("20240801123456", DateUtils.getFormattedNow(DateUtils.NUMBER_FORMATER));
    }
}