package com.kk12.vertx_distributed_service;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

public class ApiVerticle extends AbstractVerticle {
  @Override
  public void start() {
    vertx.createHttpServer().requestHandler(req -> {
      if (req.method() == HttpMethod.POST && "/process".equals(req.path())) {
        req.bodyHandler(body -> {
          JsonObject requestData = body.toJsonObject();
          // Send request to the event bus
          vertx.eventBus().request("processing.address", requestData, reply -> {
            if (reply.succeeded()) {
              req.response().putHeader("Content-Type", "application/json")
                .end(reply.result().body().toString());
            } else {
              req.response().setStatusCode(500).end("Processing failed");
            }
          });
        });
      } else {
        req.response().setStatusCode(404).end();
      }
    }).listen(8080);
  }
}
