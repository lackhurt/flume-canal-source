package com.weiboyi.etl.flume.source.canal;


import junit.framework.Assert;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.List;

public class TestCanalConf {

    @Test(expected = ServerUrlsFormatException.class)
    public void testConvertUrlsToSocketAddressListMalformed() throws ServerUrlsFormatException {
        CanalConf.convertUrlsToSocketAddressList("192.168.100.101:11111,192.168.100.101");
    }

    @Test
    public void testConvertUrlsToSocketAddressList() throws ServerUrlsFormatException {
        List<SocketAddress> list = CanalConf.convertUrlsToSocketAddressList("192.168.100.101:11111,192.168.100.102:10000,192.168.100.103:11111");

        Assert.assertEquals(list.size(), 3);
    }
}
