package com.kk12.vertx_distributed_service;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.nio.file.Files;
import java.nio.file.Paths;

public class MainVerticle extends AbstractVerticle {
  @Override
  public void start() {
    // Load configuration from file
    JsonObject config = loadConfig();

    // Determine role dynamically
    String role = ConfigUtil.getRole(config);

    if ("api".equalsIgnoreCase(role)) {
      vertx.deployVerticle(new ApiVerticle());
    } else if ("processing".equalsIgnoreCase(role)) {
      vertx.deployVerticle(new ProcessingVerticle());
    } else {
      System.err.println("Invalid role specified!");
    }
  }

  private JsonObject loadConfig() {
    try {
      String content = new String(Files.readAllBytes(Paths.get("config.json")));
      return new JsonObject(content);
    } catch (Exception e) {
      return new JsonObject(); // Fallback to empty config
    }
  }

  public static void main(String[] args) {
    // Use Hazelcast for clustering
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(new HazelcastClusterManager()), res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
        vertx.deployVerticle(new MainVerticle());
      } else {
        System.err.println("Cluster initialization failed!");
      }
    });
  }
}
