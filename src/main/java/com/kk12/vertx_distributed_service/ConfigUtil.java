package com.kk12.vertx_distributed_service;

import io.vertx.core.json.JsonObject;

public class ConfigUtil {
  public static String getRole(JsonObject config) {
    // Check environment variable or config file
    return System.getenv().getOrDefault("VERTX_ROLE", config.getString("role", "api"));
  }
}
