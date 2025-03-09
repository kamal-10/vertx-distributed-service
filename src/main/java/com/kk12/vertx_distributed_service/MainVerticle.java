package com.kk12.vertx_distributed_service;


import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

public class MainVerticle extends AbstractVerticle {

  private static HazelcastClusterManager clusterManager;
  private static HazelcastInstance hazelcastInstance;
  private static final String ROLE_MAP_NAME = "node-roles";

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
    HazelcastClusterManager clusterManager = new HazelcastClusterManager();
    VertxOptions options = new VertxOptions().setClusterManager(clusterManager);

    Vertx.clusteredVertx(options).onSuccess(vertx -> {
      System.out.println("🚀 Clustered Vert.x started!");
      vertx.deployVerticle(new MainVerticle());
    }).onFailure(err -> {
      System.err.println("❌ Failed to start clustered Vert.x: " + err.getMessage());
    });
  }


  public static void main1(String[] args) {

    clusterManager = new HazelcastClusterManager();
    VertxOptions options = new VertxOptions().setClusterManager(clusterManager);

    // Start Clustered Vert.x
    Vertx.clusteredVertx(options).onSuccess(vertx -> {
      System.out.println("Clustered Vert.x started!");

      // Get Hazelcast instance
      hazelcastInstance = clusterManager.getHazelcastInstance();

      assignRoleDynamically(vertx);

      if (hazelcastInstance != null) {
        hazelcastInstance.getCluster().addMembershipListener(new MembershipListener() {
          @Override
          public void memberAdded(MembershipEvent membershipEvent) {
            System.out.println("New node joined: " + membershipEvent.getMember());
            assignRoleDynamically(vertx);
          }

          @Override
          public void memberRemoved(MembershipEvent membershipEvent) {
            System.out.println("Node left: " + membershipEvent.getMember());
            rebalanceRoles(vertx);
          }
        });
      }
    }).onFailure(err -> {
      System.err.println("Failed to start clustered Vert.x: " + err.getMessage());
    });
  }



  private static void assignRoleDynamically(Vertx vertx) {
    vertx.setTimer(2000, id -> {  // Wait 2 seconds for Hazelcast to sync
      IMap<String, String> roleMap = hazelcastInstance.getMap(ROLE_MAP_NAME);
      String assignedRole="";
      if (roleMap.size()==0){
        String nodeId = String.valueOf(hazelcastInstance.getCluster().getLocalMember().getUuid());
        roleMap.put(nodeId, "api");
        assignedRole="api";


      }else{
//        roleMap.loadAll(true);  // Ensure we have all the data
        long apiCount = roleMap.values().stream().filter(role -> role.equals("api")).count();

        String nodeId = String.valueOf(hazelcastInstance.getCluster().getLocalMember().getUuid());

        assignedRole = (apiCount == 0) ? "api" : "processing";
        roleMap.put(nodeId, assignedRole);

        System.out.println("✅ Assigned role " + assignedRole + " to node " + nodeId);
      }



      if (assignedRole.equals("api")) {
        vertx.deployVerticle(new ApiVerticle());
      } else {
        vertx.deployVerticle(new ProcessingVerticle());
      }
    });
  }


  private static void rebalanceRoles(Vertx vertx) {
    IMap<String, String> roleMap = hazelcastInstance.getMap(ROLE_MAP_NAME);

    // Get remaining nodes
    Map<String, String> currentRoles = roleMap.getAll(roleMap.keySet());

    long apiCount = currentRoles.values().stream().filter(role -> role.equals("api")).count();

    if (apiCount == 0 && !currentRoles.isEmpty()) {
      // Promote a processing node to API
      for (Map.Entry<String, String> entry : currentRoles.entrySet()) {
        roleMap.put(entry.getKey(), "api");
        System.out.println("Promoted node " + entry.getKey() + " to API.");
        break;
      }
    }
  }
}
