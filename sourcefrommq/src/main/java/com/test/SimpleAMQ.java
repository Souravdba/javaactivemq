package com.test;


public class SimpleAMQ {
	public static void main(String[] args) {
		Runnable rr=new bb();
		Thread tt=new Thread(rr);
		tt.start();
	}
	
}
