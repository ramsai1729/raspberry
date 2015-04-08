package com.narsimhulu.raspberry;

import com.narsimhulu.aws.KinesisClient;

import java.util.List;

/**
 *
 * Get example
 *
 * @author sairam
 */
public class MotionSensorRead {
  public static void main(String[] args) {
    final KinesisClient kinesisClient = new KinesisClient("motionSensorStream");
    List<String> records = kinesisClient.getRecords();
    System.out.println(records);
  }
}
