package com.sap.hadoop.client.redis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class JedisPoolUtils {
	
	public static JedisPool pool=null;
	
	/**
	 */
	static{
		Properties prop = new Properties();
		try {
			InputStream input = new FileInputStream("redis.properties");
			prop.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(Integer.parseInt(prop.get("redis.MaxTotal").toString())); 
		poolConfig.setMinIdle(Integer.parseInt(prop.get("redis.MinIdle").toString())); 
		poolConfig.setMaxIdle(Integer.parseInt(prop.get("redis.MaxIdle").toString()));  
		pool = new JedisPool(poolConfig,prop.get("redis.host").toString(),Integer.parseInt(prop.get("redis.port").toString()));
	}
	
	/**

	 * @return
	 */
	public static Jedis getJedis(){
		Jedis jedis = pool.getResource();
		return jedis;
	}

}
