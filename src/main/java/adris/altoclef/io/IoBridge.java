package adris.altoclef.io;

import adris.altoclef.AltoClef;
import adris.altoclef.TaskCatalogue;
import adris.altoclef.eventbus.EventBus;
import adris.altoclef.eventbus.events.ChatMessageEvent;
import adris.altoclef.eventbus.events.ClientTickEvent;
import adris.altoclef.tasks.movement.GetToBlockTask;
import adris.altoclef.tasks.movement.GetToXZTask;
import adris.altoclef.tasks.movement.FollowPlayerTask;
import adris.altoclef.tasks.movement.DefaultGoToDimensionTask;
import adris.altoclef.tasks.entity.GiveItemToPlayerTask;
import adris.altoclef.tasks.resources.CollectFoodTask;
import adris.altoclef.tasks.construction.compound.ConstructNetherPortalBucketTask;
import adris.altoclef.tasksystem.Task;
import adris.altoclef.util.Dimension;
import adris.altoclef.util.ItemTarget;
import adris.altoclef.util.helpers.StorageHelper;
import adris.altoclef.util.helpers.WorldHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.network.PlayerListEntry;
import net.minecraft.client.network.ClientPlayerEntity;
import net.minecraft.block.Blocks;
import net.minecraft.item.ItemStack;
import net.minecraft.registry.Registries;
import net.minecraft.util.math.BlockPos;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * IO's end of the body protocol (open-ended plan, section 6): a WebSocket client of Core. It runs
 * what Core calls for through AltoClef and reports state, results and chat. It never decides
 * permissions; policy and the journal come later (O1.6, O1.11).
 *
 * Off unless IO_BRIDGE_TOKEN is set. IO_BRIDGE_URL defaults to ws://127.0.0.1:8766.
 */
public final class IoBridge {
    private static final String MOD_VERSION = "0.19-io";
    private static final int PROTOCOL_VERSION = 1;
    private static final long STATE_MIN_INTERVAL_MS = 250;

    private final AltoClef mod;
    private final ObjectMapper json = new ObjectMapper();
    private final String url;
    private final String token;
    private final String bodySession = UUID.randomUUID().toString();
    private final Object sendLock = new Object();

    private volatile WebSocket socket;
    private volatile String coreSession;
    private volatile boolean welcomed;
    private long seq;
    private final StringBuilder partial = new StringBuilder();

    // Only touched on the client thread.
    private String runningCallId;
    private boolean cancelling;
    private long lastStateAt;
    private String lastStateJson = "";
    private final Map<String, JsonNode> finished = new HashMap<>();

    public static void start(AltoClef mod) {
        String token = System.getenv("IO_BRIDGE_TOKEN");
        if (token == null || token.isBlank()) return;
        String url = System.getenv().getOrDefault("IO_BRIDGE_URL", "ws://127.0.0.1:8766");
        new IoBridge(mod, url, token).begin();
    }

    private IoBridge(AltoClef mod, String url, String token) {
        this.mod = mod;
        this.url = url;
        this.token = token;
    }

    private void begin() {
        EventBus.subscribe(ClientTickEvent.class, evt -> tick());
        EventBus.subscribe(ChatMessageEvent.class, evt -> onChat(evt));
        Thread connector = new Thread(this::connectLoop, "io-bridge");
        connector.setDaemon(true);
        connector.start();
        log("bridge started, Core at " + url);
    }

    private static void log(String line) {
        System.out.println("IO-BRIDGE: " + line);
    }

    // ---- connection -------------------------------------------------------------------------

    private void connectLoop() {
        HttpClient client = HttpClient.newHttpClient();
        while (true) {
            if (socket == null) {
                try {
                    welcomed = false;
                    partial.setLength(0);
                    WebSocket ws = client.newWebSocketBuilder().buildAsync(URI.create(url), new Listener()).get();
                    seq = 0;
                    socket = ws;
                    sendHello();
                } catch (Exception e) {
                    socket = null;
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private final class Listener implements WebSocket.Listener {
        @Override
        public void onOpen(WebSocket ws) {
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                String text = partial.toString();
                partial.setLength(0);
                try {
                    onMessage(text);
                } catch (Exception e) {
                    log("bad message: " + e);
                }
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
            linkDown("closed " + code);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            linkDown("error " + error);
        }
    }

    /** Core is gone: stop user work (6.3). Survival chains are left alone. */
    private void linkDown(String why) {
        log("link down: " + why);
        socket = null;
        welcomed = false;
        MinecraftClient.getInstance().execute(() -> {
            if (runningCallId != null) {
                cancelling = true;
                mod.cancelUserTask();
                runningCallId = null;
            }
        });
    }

    // ---- sending ----------------------------------------------------------------------------

    private ObjectNode envelope(String type, ObjectNode payload) {
        ObjectNode env = json.createObjectNode();
        env.put("protocolVersion", PROTOCOL_VERSION);
        env.put("type", type);
        env.put("requestId", UUID.randomUUID().toString());
        env.put("timestamp", Instant.now().toString());
        env.put("agentId", System.getenv().getOrDefault("IO_AGENT_ID", "io"));
        env.set("payload", payload);
        return env;
    }

    private void sendHello() throws Exception {
        MinecraftClient mc = MinecraftClient.getInstance();
        ObjectNode p = json.createObjectNode();
        p.put("agentId", System.getenv().getOrDefault("IO_AGENT_ID", "io"));
        p.put("username", mc.getSession() != null ? mc.getSession().getUsername() : "io");
        p.put("gameVersion", mc.getGameVersion());
        p.put("modVersion", MOD_VERSION);
        p.put("bodySession", bodySession);
        ObjectNode journal = p.putObject("journal");
        ArrayNode running = journal.putArray("running");
        if (runningCallId != null) running.add(runningCallId);
        journal.putArray("finished");
        p.put("token", token);
        WebSocket ws = socket;
        if (ws != null) ws.sendText(json.writeValueAsString(envelope("body.hello", p)), true);
    }

    /** Every message after the hello carries both sessions and a per-sender seq that only goes up. */
    private void send(String type, ObjectNode payload) {
        WebSocket ws = socket;
        if (ws == null || !welcomed) return;
        synchronized (sendLock) {
            payload.put("coreSession", coreSession);
            payload.put("bodySession", bodySession);
            payload.put("seq", seq++);
            try {
                ws.sendText(json.writeValueAsString(envelope(type, payload)), true);
            } catch (Exception e) {
                log("send failed: " + e);
            }
        }
    }

    // ---- receiving --------------------------------------------------------------------------

    private void onMessage(String text) throws Exception {
        JsonNode env = json.readTree(text);
        String type = env.path("type").asText();
        JsonNode p = env.path("payload");
        switch (type) {
            case "core.welcome" -> {
                coreSession = p.path("coreSession").asText();
                welcomed = true;
                log("welcomed by Core session " + coreSession);
                // A new Core connection needs a snapshot even if the player has not moved.
                MinecraftClient.getInstance().execute(() -> pushState(true));
            }
            case "ping" -> {
                ObjectNode pong = json.createObjectNode();
                pong.put("at", Instant.now().toString());
                send("pong", pong);
            }
            case "body.call" -> MinecraftClient.getInstance().execute(() -> onCall(p));
            case "body.cancel" -> MinecraftClient.getInstance().execute(() -> onCancel(p.path("callId").asText()));
            default -> log("ignored " + type);
        }
    }

    // ---- calls (client thread) --------------------------------------------------------------

    private void onCall(JsonNode p) {
        String callId = p.path("callId").asText();
        String op = p.path("op").asText();
        JsonNode args = p.path("args");

        // A callId already seen is never run again: say what it came to (6.2).
        JsonNode known = finished.get(callId);
        if (known != null) {
            send("body.result", (ObjectNode) known.deepCopy());
            return;
        }
        if (callId.equals(runningCallId)) {
            ObjectNode a = json.createObjectNode();
            a.put("callId", callId);
            send("body.accepted", a);
            return;
        }
        if (runningCallId != null) {
            result(callId, "failed", "busy", null);
            return;
        }
        ObjectNode accepted = json.createObjectNode();
        accepted.put("callId", callId);
        send("body.accepted", accepted);

        try {
            switch (op) {
                case "task.get" -> startGet(callId, args.path("item").asText(), args.path("count").asInt(1));
                case "task.goto" -> startGoto(callId, args);
                case "task.food" -> startFood(callId, args.path("count").asInt(20));
                case "task.follow" -> startFollow(callId, args.path("player").asText());
                case "task.give" -> startGive(callId, args);
                case "task.portal" -> startPortal(callId, args.path("enter").asBoolean(false));
                default -> result(callId, "failed", "unsupported_op", null);
            }
        } catch (Exception e) {
            runningCallId = null;
            result(callId, "error", String.valueOf(e), null);
        }
    }

    private void startGet(String callId, String item, int count) {
        if (!TaskCatalogue.taskExists(item)) {
            result(callId, "failed", "unknown_item", null);
            return;
        }
        Task task = TaskCatalogue.getItemTask(item, count);
        beginTask(callId, task, () -> {
            int have = countOf(item);
            ObjectNode value = json.createObjectNode();
            value.put("item", item);
            value.put("count", have);
            if (have >= count) result(callId, "ok", null, value);
            else result(callId, "failed", "incomplete", value);
        }, () -> {
            ObjectNode partial = json.createObjectNode();
            partial.put("item", item);
            partial.put("have", countOf(item));
            return partial;
        });
    }

    private void startGoto(String callId, JsonNode args) {
        int x = args.path("x").asInt();
        int z = args.path("z").asInt();
        boolean hasY = args.has("y");
        int y = args.path("y").asInt();
        Task task = hasY ? new GetToBlockTask(new BlockPos(x, y, z)) : new GetToXZTask(x, z);
        beginTask(callId, task, () -> {
            ClientPlayerEntity player = MinecraftClient.getInstance().player;
            double dx = player.getX() - x - 0.5;
            double dz = player.getZ() - z - 0.5;
            double dy = hasY ? player.getY() - y : 0;
            double dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
            ObjectNode value = json.createObjectNode();
            value.put("distance", dist);
            if (dist <= 3.0) result(callId, "ok", null, value);
            else result(callId, "failed", "not_arrived", value);
        }, () -> json.createObjectNode());
    }

    /** count is inventory hunger units, as in AltoClef's @food command. */
    private void startFood(String callId, int count) {
        if (count <= 0 || count > 10000) throw new IllegalArgumentException("invalid food count");
        PartialSupplier food = () -> {
            ObjectNode value = json.createObjectNode();
            value.put("foodUnits", StorageHelper.calculateInventoryFoodScore());
            return value;
        };
        beginTask(callId, new CollectFoodTask(count), () -> {
            ObjectNode value = food.get();
            result(callId, value.path("foodUnits").asInt() >= count ? "ok" : "failed", null, value);
        }, food);
    }

    private ObjectNode playerDistance(String player) {
        ObjectNode value = json.createObjectNode();
        value.put("player", player);
        var target = mod.getEntityTracker().getPlayerEntity(player);
        if (target.isPresent()) value.put("distance", target.get().distanceTo(mod.getPlayer()));
        else value.putNull("distance");
        return value;
    }

    /** Following is continuous; Core cancels it after independently checking the follower. */
    private void startFollow(String callId, String player) {
        if (!player.matches("[A-Za-z0-9_]{1,16}")) throw new IllegalArgumentException("invalid player");
        beginTask(callId, new FollowPlayerTask(player),
                () -> result(callId, "failed", "follow_ended", playerDistance(player)),
                () -> playerDistance(player));
    }

    /** The result reports dropped items; the operator must independently verify pickup. */
    private void startGive(String callId, JsonNode args) {
        String player = args.path("player").asText();
        String item = args.path("item").asText();
        int count = args.path("count").asInt(1);
        if (!player.matches("[A-Za-z0-9_]{1,16}") || count <= 0 || count > 2304) {
            throw new IllegalArgumentException("invalid give arguments");
        }
        if (!TaskCatalogue.taskExists(item)) {
            result(callId, "failed", "unknown_item", null);
            return;
        }
        int before = countOf(item);
        GiveItemToPlayerTask task = new GiveItemToPlayerTask(player, new ItemTarget(item, count));
        beginTask(callId, task, () -> {
            ObjectNode value = json.createObjectNode();
            value.put("player", player);
            value.put("item", item);
            value.put("remaining", countOf(item));
            value.put("inventoryDecrease", before - countOf(item));
            value.put("dropped", task.getDroppedCount());
            result(callId, task.getDroppedCount() == count ? "ok" : "failed",
                    task.getDroppedCount() == count ? null : "give_incomplete", value);
        }, () -> {
            ObjectNode value = playerDistance(player);
            value.put("remaining", countOf(item));
            return value;
        });
    }

    private BlockPos nearestPortal() {
        var portal = mod.getBlockScanner().getNearestBlock(Blocks.NETHER_PORTAL);
        if (portal.isEmpty() || !portal.get().isWithinDistance(mod.getPlayer().getPos(), 32)) return null;
        return mod.getWorld().getBlockState(portal.get()).getBlock() == Blocks.NETHER_PORTAL ? portal.get() : null;
    }

    /** BUILD_PORTAL_VANILLA uses the bucket task; enter=true also crosses into the Nether. */
    private void startPortal(String callId, boolean enter) {
        if (WorldHelper.getCurrentDimension() != Dimension.OVERWORLD) {
            result(callId, "failed", "start_in_overworld", null);
            return;
        }
        Task task = enter ? new DefaultGoToDimensionTask(Dimension.NETHER) : new ConstructNetherPortalBucketTask() {
            @Override
            public boolean isFinished() {
                return nearestPortal() != null;
            }
        };
        PartialSupplier observed = () -> {
            ObjectNode value = json.createObjectNode();
            value.put("dimension", WorldHelper.getCurrentDimension().toString());
            BlockPos portal = nearestPortal();
            if (portal != null) {
                ObjectNode pos = value.putObject("portal");
                pos.put("x", portal.getX());
                pos.put("y", portal.getY());
                pos.put("z", portal.getZ());
            }
            return value;
        };
        beginTask(callId, task, () -> {
            boolean success = enter ? WorldHelper.getCurrentDimension() == Dimension.NETHER : nearestPortal() != null;
            result(callId, success ? "ok" : "failed", success ? null : "portal_incomplete", observed.get());
        }, observed);
    }

    private interface PartialSupplier {
        ObjectNode get();
    }

    private PartialSupplier currentPartial = () -> json.createObjectNode();

    private void beginTask(String callId, Task task, Runnable onDone, PartialSupplier partialNow) {
        runningCallId = callId;
        cancelling = false;
        currentPartial = partialNow;
        mod.runUserTask(task, () -> {
            // The user chain also calls this when it is cancelled: then onCancel answers.
            if (cancelling) return;
            if (!callId.equals(runningCallId)) return;
            runningCallId = null;
            onDone.run();
        });
    }

    private void onCancel(String callId) {
        ObjectNode out = json.createObjectNode();
        out.put("callId", callId);
        if (callId.equals(runningCallId)) {
            cancelling = true;
            ObjectNode partialValue = currentPartial.get();
            mod.cancelUserTask();
            runningCallId = null;
            out.put("phase", "stopped");
            out.set("partial", partialValue);
            send("body.cancelled", out);
            pushState(true);
            return;
        }
        out.put("phase", finished.containsKey(callId) ? "already_finished" : "unknown_call");
        send("body.cancelled", out);
    }

    private void result(String callId, String status, String reason, JsonNode value) {
        ObjectNode out = json.createObjectNode();
        out.put("callId", callId);
        out.put("status", status);
        if (reason != null) out.put("reason", reason);
        if (value != null) out.set("value", value);
        finished.put(callId, out.deepCopy());
        send("body.result", out);
        pushState(true);
    }

    private int countOf(String item) {
        ClientPlayerEntity player = MinecraftClient.getInstance().player;
        if (player == null) return 0;
        int total = 0;
        for (Map.Entry<String, Integer> e : inventory(player).entrySet()) {
            if (e.getKey().equals(item)) total += e.getValue();
        }
        return total;
    }

    // ---- state and events -------------------------------------------------------------------

    private Map<String, Integer> inventory(ClientPlayerEntity player) {
        Map<String, Integer> items = new TreeMap<>();
        for (int i = 0; i < player.getInventory().size(); i++) {
            ItemStack stack = player.getInventory().getStack(i);
            if (stack.isEmpty()) continue;
            items.merge(Registries.ITEM.getId(stack.getItem()).getPath(), stack.getCount(), Integer::sum);
        }
        return items;
    }

    private void tick() {
        if (welcomed) pushState(false);
    }

    private void pushState(boolean force) {
        long now = System.currentTimeMillis();
        if (!force && now - lastStateAt < STATE_MIN_INTERVAL_MS) return;
        MinecraftClient mc = MinecraftClient.getInstance();
        ClientPlayerEntity player = mc.player;
        ObjectNode p = json.createObjectNode();
        if (player == null || mc.world == null) {
            p.putNull("position");
            p.putNull("dimension");
            p.putNull("health");
            p.putNull("food");
            p.putArray("inventory");
        } else {
            ObjectNode pos = p.putObject("position");
            pos.put("x", player.getX());
            pos.put("y", player.getY());
            pos.put("z", player.getZ());
            p.put("dimension", mc.world.getRegistryKey().getValue().toString());
            p.put("health", player.getHealth());
            p.put("food", player.getHungerManager().getFoodLevel());
            ArrayNode inv = p.putArray("inventory");
            for (Map.Entry<String, Integer> e : inventory(player).entrySet()) {
                ObjectNode row = inv.addObject();
                row.put("item", e.getKey());
                row.put("count", e.getValue());
            }
        }
        p.putObject("equipment");
        ArrayNode running = p.putArray("running");
        if (runningCallId != null) running.add(runningCallId);
        p.put("chain", runningCallId != null ? "User Tasks" : null);

        // Pushed on change, at most four a second.
        String compare = p.toString();
        if (!force && compare.equals(lastStateJson)) return;
        lastStateJson = compare;
        lastStateAt = now;
        send("body.state", p);
    }

    private void onChat(ChatMessageEvent evt) {
        if (!welcomed) return;
        String sender = evt.senderName();
        ObjectNode p = json.createObjectNode();
        p.put("kind", "chat");
        p.put("at", Instant.now().toString());
        if (sender != null) {
            p.put("username", sender);
            var handler = MinecraftClient.getInstance().getNetworkHandler();
            PlayerListEntry entry = handler == null ? null : handler.getPlayerListEntry(sender);
            if (entry != null) p.put("uuid", entry.getProfile().getId().toString());
        }
        p.put("text", evt.messageContent());
        send("body.event", p);
    }
}
