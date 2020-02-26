package org.sireum.hamr.inspector.capabilities.redis;

import art.Art;
import art.DataContent;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.sireum.hamr.inspector.capabilities.InspectorHAMRLauncher$;

public final class Commands {

  public static final String NUM_SESSIONS_KEY = "numSessions";

  private static RedisCommands<String, String> cmds = null;
  private static String startKey = null;
  private static String stopKey = null;
  private static String streamKey = null;

  public static String incomingChannelKey = null;
  private static final String outgoingChannelKey = "inspector-channel";

  public static synchronized void init(RedisCommands<String, String> cmds) {
    if (startKey != null || stopKey != null || streamKey != null || incomingChannelKey != null) {
      throw new IllegalStateException("Commands.init() in inspector-capabilities-redis can only be initialized once!");
    }

    Commands.cmds = cmds;

    // if the redis server does NOT contain NUM_SESSIONS_KEY, then initialize it to 0
    cmds.setnx(NUM_SESSIONS_KEY, "0");

    // atomically increment session counter, returning this session's unique key
    final String streamName = Long.toString(cmds.incr(NUM_SESSIONS_KEY));
    startKey = streamName + "-start";
    stopKey = streamName + "-stop";
    streamKey = streamName + "-stream";
    incomingChannelKey = streamName + "-channel";

    // add shutdown hook to set shutdown time if the app did not stop naturally
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      stop(Art.time().toString(), "JVM Shutdown Hook");
    }));
  }

  public static void start(String timestamp) {
    if (startKey == null || stopKey == null || streamKey == null || incomingChannelKey == null) {
      throw new IllegalStateException("Commands.init() must be called before Commands.start() in inspector-capabilities-redis.");
    }

    // set the value of startKey, but only if it does not already exist
    final String reply = cmds.set(startKey, timestamp, SetArgs.Builder.nx());

    // throw error if something went wrong
    if (!reply.equals("OK")) {
      throw new IllegalStateException("Commands.start() in inspector-capabilities-redis was unable to set startKey");
    }

    cmds.publish(outgoingChannelKey, startKey);
  }

  public static void stop(String timestamp, String cause) {
    if (startKey == null || stopKey == null || streamKey == null || incomingChannelKey == null) {
      throw new IllegalStateException("Commands.init() must be called before Commands.stop() in inspector-capabilities-redis.");
    }

    // set the value of stopKey, but only if it does not already exist
    final String reply = cmds.set(stopKey, timestamp, SetArgs.Builder.nx());

    // Tell the Inspector that this project is shutting down, but only if this is the first time calling the method.
    // This is because shutdown hook and natural shutdown may cause two calls to this method.
    if (reply.equals("OK")) {
      cmds.publish(outgoingChannelKey, stopKey);
      cmds.xadd(streamKey, "timestamp", timestamp, "stop", cause);
    }
  }

  public static void xadd(String timestamp, long src, long dst, DataContent data) {
    final String json = InspectorHAMRLauncher$.MODULE$.serializer().apply(data);
    cmds.xadd(streamKey, "timestamp", timestamp, "src", Long.toString(src), "dst", Long.toString(dst), "data", json);
  }

  private Commands() {
    throw new IllegalStateException("Commands cannot be instantiated.");
  }

}
