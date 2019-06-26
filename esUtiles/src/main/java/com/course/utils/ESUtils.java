package com.course.utils;


import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Administrator on 2019/6/26.
 */
public class ESUtils {

    public static TransportClient client = null;

    /**
     * 对外提供 client
     * @return
     */
    public TransportClient getClient() {
        if (client!=null){
            return client;
        }
        client = new ESUtils().initESClient();
        return client;
    }
    private TransportClient initESClient() {
        // 配置你的es,如果你的集群名称不是默认的elasticsearch，需要以下这步
        Settings settings = Settings.builder().put("cluster.name", "elastic")
                .put("node.name","elastic-node-1")
                .build();
        // 这里可以同时连接集群的服务器,可以多个,并且连接服务是可访问的
        try {
            // 创建client
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 19300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                System.out.println("连接成功！");
            }
        }
        return client;
    }

    /**
     * close es client
     * @param client
     */
    public void closeESClient(Client client) {
        if(client !=null) {
            try {
                client.close();
            }catch (Exception e) {}
            client = null;
        }
        System.out.println("连接关闭！" + client);
    }
}
