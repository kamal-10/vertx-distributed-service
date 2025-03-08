package com.kk12.vertx_distributed_service;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

public class ProcessingVerticle extends AbstractVerticle {
  @Override
  public void start() {
    vertx.eventBus().consumer("processing.address", message -> {
      JsonObject data = (JsonObject) message.body();
      // Simulate processing
      data.put("status", "processed");
      // Send response back
      message.reply(data);
    });
  }
}
