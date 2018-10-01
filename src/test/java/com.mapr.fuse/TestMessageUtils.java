package com.mapr.fuse;

import com.mapr.fuse.dto.MessageConfig;
import com.mapr.fuse.utils.MessageUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMessageUtils {

    public MessageConfig config = new MessageConfig("START", "END", "|", true);
    public int configLength = 9;

    @Test
    public void getSeparatorsLengthTest() {
        assertEquals(configLength, MessageUtils.getSeparatorsLength(config));
    }

    @Test
    public void formatMessageWithSizeTest() {
        String message = "test";
        String expected = "START4testEND|";

        String formattedMessage = MessageUtils.formatMessage(config, message, false);

        assertEquals(expected, formattedMessage);
    }

    @Test
    public void formatMessageWithoutSize() {
        MessageConfig config = new MessageConfig("START", "END", "|", false);

        String message = "test";
        String expected = "STARTtestEND|";

        String formattedMessage = MessageUtils.formatMessage(config, message, false);

        assertEquals(expected, formattedMessage);
    }

    @Test
    public void formateMessageWithSizeAndCutTest() {
        String message = "test";
        String expected = "testEND|";

        String formattedMessage = MessageUtils.formatMessage(config, message, true);

        assertEquals(expected, formattedMessage);
    }

}
