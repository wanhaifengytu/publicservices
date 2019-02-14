package com.sap.hadoop.test.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestExecutorThreads {
	
	public static void main(String[] args) {
		System.out.println("This is the start of the executor thread");
		addFixedNumberThreadPool(6);
		
		addCachedThreadPool();
		
		addScheduledThreadPool();
		addscheduleAtFixedRate();
		addNewSingleThreadExecutor();
		
		addScheduleWithFixedDelay();
	}
	
	
	private static void addFixedNumberThreadPool(int threadCount) {
		ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < 20; i++) {
            
        	Runnable syncRunnable = new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                 /*   try {
						Thread.sleep(600);
					} catch (InterruptedException e) {
						e.printStackTrace();
					} */
                }
            };
           
            executorService.execute(syncRunnable);
        }
        
	}

	
	private static void addCachedThreadPool() {
		 ExecutorService executorService = Executors.newCachedThreadPool();
	        for (int i = 0; i < 100; i++) {
	            Runnable syncRunnable = new Runnable() {
	                @Override
	                public void run() {
	                	System.out.println(Thread.currentThread().getName());
	                }
	            };
	            executorService.execute(syncRunnable);
	        }
	}
	
	
	
	private static void addScheduledThreadPool() {
		  ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
	        for (int i = 0; i < 20; i++) {
	            Runnable syncRunnable = new Runnable() {
	                @Override
	                public void run() {
	                	System.out.println( Thread.currentThread().getName());
	                }
	            };
	            executorService.schedule(syncRunnable, 5000, TimeUnit.MILLISECONDS);
	        }
	        
	}
	
	
	private static void addscheduleAtFixedRate() {
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
        Runnable syncRunnable = new Runnable() {
            @Override
            public void run() {
            	System.out.println(Thread.currentThread().getName());
            }
        };
        executorService.scheduleAtFixedRate(syncRunnable, 5000, 3000, TimeUnit.MILLISECONDS);	
	}
	
	private static void addNewSingleThreadExecutor(){
		ExecutorService executorService = Executors.newSingleThreadExecutor();
        for (int i = 0; i < 20; i++) {
            Runnable syncRunnable = new Runnable() {
                @Override
                public void run() {
                	System.out.println(Thread.currentThread().getName());
                }
            };
            executorService.execute(syncRunnable);
        }
	}
	
	private static void addScheduleWithFixedDelay() {
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
        Runnable syncRunnable = new Runnable() {
            @Override
            public void run() {
            	System.out.println(Thread.currentThread().getName());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        executorService.scheduleWithFixedDelay(syncRunnable, 5000, 3000, TimeUnit.MILLISECONDS);
	}
	
}
