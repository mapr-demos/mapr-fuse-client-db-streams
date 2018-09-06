package com.mapr.fuse;

import com.mapr.fuse.utils.UserUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.attribute.UserPrincipalNotFoundException;

public class TestUserUtils {

    @Test
    public void getCorrectUidTest() throws UserPrincipalNotFoundException {
        long uid = UserUtils.getUid("root");
        Assert.assertEquals(0, uid);
    }

    @Test
    public void getCorrectGidTest() throws UserPrincipalNotFoundException {
        long gid = UserUtils.getGid("root");
        Assert.assertEquals(0, gid);
    }

    @Test(expected = UserPrincipalNotFoundException.class)
    public void getWrongUidTest() throws UserPrincipalNotFoundException {
        UserUtils.getUid("sdfdsf");
    }

    @Test(expected = UserPrincipalNotFoundException.class)
    public void getWrongGidTest() throws UserPrincipalNotFoundException {
        UserUtils.getGid("sdfdsf");
    }

}
