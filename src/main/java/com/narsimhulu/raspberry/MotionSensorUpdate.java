package com.narsimhulu.raspberry;


import com.narsimhulu.aws.KinesisClient;
import com.pi4j.io.gpio.*;
import com.pi4j.io.gpio.event.GpioPinListenerDigital;

/**
 * Put example
 *
 * @author sairam
 */
public class MotionSensorUpdate {

  public static void main(String[] args) throws InterruptedException {

    final KinesisClient kinesisClient = new KinesisClient("motionSensorStream");

    System.out.println("Started MotionSensorUpdate...");

    final GpioController gpio = GpioFactory.getInstance();

    final GpioPinDigitalInput myButton = gpio.provisionDigitalInputPin(RaspiPin.GPIO_07, PinPullResistance.PULL_DOWN);
    myButton.addListener((GpioPinListenerDigital) event -> {
      // display pin state on console
      System.out.println("GPIO state has changed: " + event.getPin() + " = " + event.getState() + ", Updating amazon cloud");
      kinesisClient.putRecord(event.getState().getName() + " @ " + System.currentTimeMillis());
    });
    while(true) { Thread.sleep(1000); }
  }
}

