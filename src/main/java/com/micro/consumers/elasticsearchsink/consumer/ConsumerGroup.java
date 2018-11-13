package com.micro.consumers.elasticsearchsink.consumer;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {
 
  private final int numberOfConsumers;
  private List<ConsumerThread> consumers;
 
  public ConsumerGroup(ConsumerThread ncThread, int numberOfConsumers) {
    this.numberOfConsumers = numberOfConsumers;
    consumers = new ArrayList<>();
    for (int i = 0; i < this.numberOfConsumers; i++) {
      consumers.add(ncThread);
    }
  }
 
  public void execute() {
    for (ConsumerThread ncThread : consumers) {
      Thread t = new Thread(ncThread);
      t.start();
    }
  }
 
  public int getNumberOfConsumers() {
    return numberOfConsumers;
  }
}