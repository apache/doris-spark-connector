package org.apache.doris.spark.util;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EscapeHandler {
    public static final String ESCAPE_DELIMITERS_FLAGS = "\\x";
    public static final Pattern ESCAPE_PATTERN = Pattern.compile("\\\\x([0-9|a-f|A-F]{2})");

    public static String escapeString(String source) {
        if (source.contains(ESCAPE_DELIMITERS_FLAGS)) {
            Matcher m = ESCAPE_PATTERN.matcher(source);
            StringBuffer buf = new StringBuffer();
            while (m.find()) {
                m.appendReplacement(buf, String.format("%s", (char) Integer.parseInt(m.group(1), 16)));
            }
            m.appendTail(buf);
            return buf.toString();
        }
        return source;
    }

}