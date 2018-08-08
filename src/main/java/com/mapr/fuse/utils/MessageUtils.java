package com.mapr.fuse.utils;

import com.mapr.fuse.dto.MessageConfig;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MessageUtils {

    public static int getSeparatorsLength(MessageConfig messageConfig) {
        return (messageConfig.getSeparator() + messageConfig.getStart() + messageConfig.getStop()).getBytes().length;
    }

    public static String formatMessage(MessageConfig messageConfig, String message, boolean cutStart) {
        if(cutStart)
            return String.format("%s%s%s", message, messageConfig.getStop(), messageConfig.getSeparator());
        else if(messageConfig.getSize())
            return String.format("%s%d%s%s%s", messageConfig.getStart(), message.length(), message,
                    messageConfig.getStop(), messageConfig.getSeparator());
        else
            return String.format("%s%s%s%s", messageConfig.getStart(), message, messageConfig.getStop(),
                    messageConfig.getSeparator());

    }
}
