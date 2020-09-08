package com.sap.hadoop.client.redis;

import redis.clients.jedis.Jedis;

public class TestRedisClient {
	
	public static void main(String[] args) {
		System.out.println("This is the start of test redis client");
		Jedis jedis =  JedisPoolUtils.getJedis(); //new Jedis("10.116.29.72",6380); 
		 
		 System.out.println("Redis get " + jedis.get("runoobkeyTest"));
		 //jedis.set("runoobkeyTest", "www.runoob.com");
		 //System.out.println("Redis get another one " + jedis.get("nextKey"));
		 
		 jedis.close();
	}
	
	
	

}
