package com.mapr.fuse.utils;

import lombok.experimental.UtilityClass;
import java.io.InputStream;
import java.nio.file.attribute.UserPrincipalNotFoundException;

@UtilityClass
public class UserUtils {

    public long getUid(String userName) throws UserPrincipalNotFoundException {
        return getIdInfo("-u", userName);
    }
    public static long getGid(String userName) throws UserPrincipalNotFoundException {
        return getIdInfo("-g", userName);
    }

    private static long getIdInfo(String option, String username) throws UserPrincipalNotFoundException {
        StringBuilder output = new StringBuilder();
        String command =  String.format("id %s %s", option, username);
        try {
            Process child = Runtime.getRuntime().exec(command);
            InputStream in = child.getInputStream();
            int c;
            while ((c = in.read()) != -1) {
                output.append((char) c);
            }
            in.close();
            String result = output.toString();
            return Long.parseLong(result.substring(0, result.length() - 1));
        } catch (Exception ex) {
            throw new UserPrincipalNotFoundException(username);
        }
    }

}