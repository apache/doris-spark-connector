package org.apache.doris.spark.util;

import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Properties;

public class EscapeHandlerTest extends TestCase {

    public void testEscapeString() {


        String s1 = "\\x09\\x09";
        String s2 = "\\x0A\\x0A";
        Assert.assertEquals("\t\t", EscapeHandler.escapeString(s1));
        Assert.assertEquals("\n\n", EscapeHandler.escapeString(s2));

    }
}