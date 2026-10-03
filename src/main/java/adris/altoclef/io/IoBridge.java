package adris.altoclef.io;

import adris.altoclef.AltoClef;
import adris.altoclef.TaskCatalogue;
import adris.altoclef.eventbus.EventBus;
import adris.altoclef.eventbus.events.ChatMessageEvent;
import adris.altoclef.eventbus.events.ClientTickEvent;
import adris.altoclef.tasks.movement.GetToBlockTask;
import adris.altoclef.tasks.movement.GetToXZTask;
import adris.altoclef.tasks.movement.GetToEntityTask;
import adris.altoclef.tasks.movement.DefaultGoToDimensionTask;
import adris.altoclef.tasks.entity.GiveItemToPlayerTask;
import adris.altoclef.tasks.resources.CollectFoodTask;
import adris.altoclef.tasks.construction.compound.ConstructNetherPortalBucketTask;
import adris.altoclef.tasksystem.Task;
import adris.altoclef.tasksystem.ITaskRequiresGrounded;
import baritone.api.BaritoneAPI;
import baritone.api.schematic.IStaticSchematic;
import java.io.FileInputStream;
import java.nio.file.Path;
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
    private boolean usedCraftingTable;
    private static net.minecraft.item.Item smeltFuel;
    public static net.minecraft.item.Item selectedSmeltFuel() { return smeltFuel; }
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
                smeltFuel = null;
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
        if (op.startsWith("query.")) {
            try { query(callId, op, args); }
            catch (Exception e) { result(callId, "error", String.valueOf(e), null); }
            return;
        }

        // Chat must remain usable while a task owns the body (progress, stop, approvals).
        if ("prim.chat".equals(op)) {
            String text = args.path("text").asText();
            if (text.isBlank() || text.length() > 256 || text.startsWith("/") || text.contains("\n") || text.contains("\r")) {
                result(callId, "failed", "invalid_chat", null);
                return;
            }
            ObjectNode ack = json.createObjectNode();
            ack.put("callId", callId);
            send("body.accepted", ack);
            MinecraftClient.getInstance().player.networkHandler.sendChatMessage(text);
            result(callId, "ok", null, json.createObjectNode());
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
                case "task.get" -> {
                    if (args.has("smeltInput")) startSmelt(callId, args);
                    else startGet(callId, args.path("item").asText(), args.path("count").asInt(1), args.path("additional").asBoolean(false));
                }
                case "task.goto" -> startGoto(callId, args);
                case "prim.equip" -> startEquip(callId, args);
                case "prim.dig" -> startDig(callId, args);
                case "prim.place" -> startPlace(callId, args);
                case "task.food" -> startFood(callId, args.path("count").asInt(20));
                case "task.follow" -> startFollow(callId, args);
                case "task.give" -> startGive(callId, args);
                case "baritone.clearArea" -> startClearArea(callId, args);
                case "baritone.build" -> startBuild(callId, args);
                case "task.portal" -> startPortal(callId, args.path("enter").asBoolean(false));
                default -> result(callId, "failed", "unsupported_op", null);
            }
        } catch (Exception e) {
            runningCallId = null;
            smeltFuel = null;
            result(callId, "error", String.valueOf(e), null);
        }
    }

    private boolean isLoaded(BlockPos pos) {
        // ClientWorld can supply EmptyChunk/void_air for absent chunks. Never create one here.
        return mod.getWorld().getChunkManager().getChunk(pos.getX() >> 4, pos.getZ() >> 4,
                net.minecraft.world.chunk.ChunkStatus.FULL, false) != null;
    }

    /** Read-only observations do not compete with the active task. Unloaded blocks stay absent. */
    private void query(String callId, String op, JsonNode args) {
        ObjectNode value = json.createObjectNode();
        if ("query.catalog".equals(op)) {
            String item = args.path("item").asText();
            if (!item.matches("[a-z0-9_]{1,128}")) throw new IllegalArgumentException("invalid item");
            value.put("item", item);
            value.put("canGet", TaskCatalogue.taskExists(item));
            value.put("validItem", Registries.ITEM.containsId(net.minecraft.util.Identifier.of("minecraft", item)));
            value.put("validBlock", Registries.BLOCK.containsId(net.minecraft.util.Identifier.of("minecraft", item))
                    && Registries.ITEM.get(net.minecraft.util.Identifier.of("minecraft", item)) instanceof net.minecraft.item.BlockItem);
            var recipe = smeltingRecipe(item);
            if (recipe.isPresent()) value.put("smeltOutput", net.minecraft.registry.Registries.ITEM.getId(
                    recipe.get().value().getResult(mod.getWorld().getRegistryManager()).getItem()).getPath());
            else value.putNull("smeltOutput");
        } else if ("query.blockAt".equals(op)) {
            BlockPos pos = readPosition(args.path("pos"));
            boolean loaded = isLoaded(pos);
            value.put("loaded", loaded);
            if (loaded) value.put("name", net.minecraft.registry.Registries.BLOCK.getId(
                    mod.getWorld().getBlockState(pos).getBlock()).getPath());
        } else if ("query.region".equals(op)) {
            BlockPos a = readPosition(args.path("from")), b = readPosition(args.path("to"));
            BlockPos from = new BlockPos(Math.min(a.getX(), b.getX()), Math.min(a.getY(), b.getY()), Math.min(a.getZ(), b.getZ()));
            BlockPos to = new BlockPos(Math.max(a.getX(), b.getX()), Math.max(a.getY(), b.getY()), Math.max(a.getZ(), b.getZ()));
            long volume = ((long) to.getX() - from.getX() + 1) * ((long) to.getY() - from.getY() + 1) * ((long) to.getZ() - from.getZ() + 1);
            if (volume > 32768) throw new IllegalArgumentException("query region exceeds 32768 blocks");
            ObjectNode blocks = value.putObject("blocks");
            ObjectNode solid = args.path("details").asBoolean(false) ? value.putObject("solidBlocks") : null;
            if (solid != null && volume > 4096) throw new IllegalArgumentException("detailed region exceeds 4096 blocks");
            boolean complete = true;
            for (BlockPos pos : BlockPos.iterate(from, to)) {
                if (!isLoaded(pos)) { complete = false; continue; }
                blocks.put(pos.getX() + "," + pos.getY() + "," + pos.getZ(), net.minecraft.registry.Registries.BLOCK.getId(
                        mod.getWorld().getBlockState(pos).getBlock()).getPath());
                if (solid != null) solid.put(pos.getX() + "," + pos.getY() + "," + pos.getZ(),
                        mod.getWorld().getBlockState(pos).isSolidBlock(mod.getWorld(), pos));
            }
            value.put("complete", complete);
        } else if ("query.entities".equals(op)) {
            ArrayNode entities = value.putArray("entities");
            for (var entity : mod.getWorld().getEntities()) {
                ObjectNode entry = entities.addObject();
                entry.put("id", String.valueOf(entity.getId()));
                if (entity instanceof net.minecraft.entity.player.PlayerEntity player) {
                    entry.put("username", player.getName().getString());
                    entry.put("uuid", player.getUuid().toString());
                }
                entry.put("type", net.minecraft.registry.Registries.ENTITY_TYPE.getId(entity.getType()).getPath());
                ObjectNode pos = entry.putObject("pos");
                pos.put("x", entity.getX()); pos.put("y", entity.getY()); pos.put("z", entity.getZ());
            }
        } else { result(callId, "failed", "unsupported_op", null); return; }
        ObjectNode ack = json.createObjectNode(); ack.put("callId", callId); send("body.accepted", ack);
        result(callId, "ok", null, value);
    }

    private java.util.Optional<net.minecraft.recipe.RecipeEntry<net.minecraft.recipe.SmeltingRecipe>> smeltingRecipe(String input) {
        var id = net.minecraft.util.Identifier.of("minecraft", input);
        if (!net.minecraft.registry.Registries.ITEM.containsId(id)) return java.util.Optional.empty();
        var item = net.minecraft.registry.Registries.ITEM.get(id);
        return mod.getPlayer().networkHandler.getRecipeManager().getFirstMatch(net.minecraft.recipe.RecipeType.SMELTING,
                new net.minecraft.recipe.input.SingleStackRecipeInput(new net.minecraft.item.ItemStack(item)), mod.getWorld());
    }

    private void startPlace(String callId, JsonNode args) {
        BlockPos pos = readPosition(args.path("pos"));
        String name = args.path("block").asText();
        var id = net.minecraft.util.Identifier.of("minecraft", name);
        if (!Registries.BLOCK.containsId(id) || !(Registries.ITEM.get(id) instanceof net.minecraft.item.BlockItem)) {
            result(callId, "failed", "unknown_block", null); return;
        }
        var task = new IoPlaceTask(pos, Registries.BLOCK.get(id), name);
        PartialSupplier observed = () -> {
            ObjectNode value = json.createObjectNode().put("block", name);
            value.putObject("pos").put("x", pos.getX()).put("y", pos.getY()).put("z", pos.getZ());
            return value;
        };
        beginTask(callId, task, () -> result(callId, task.failure == null ? "ok" : "failed", task.failure, observed.get()), observed);
    }

    private final class IoPlaceTask extends Task implements ITaskRequiresGrounded {
        private final BlockPos pos;
        private final net.minecraft.block.Block block;
        private final String name;
        private final Task place;
        private String failure;
        IoPlaceTask(BlockPos pos, net.minecraft.block.Block block, String name) {
            this.pos = pos; this.block = block; this.name = name;
            place = new adris.altoclef.tasks.construction.PlaceBlockTask(pos, block);
        }
        @Override protected void onStart() { }
        @Override protected Task onTick() {
            if (!isLoaded(pos)) { failure = "place_failed"; return null; }
            var state = mod.getWorld().getBlockState(pos);
            if (!state.isReplaceable() || !state.getFluidState().isEmpty()) { failure = "position_occupied"; return null; }
            if (countOf(name) < 1) { failure = "not_in_inventory"; return null; }
            boolean support = false;
            for (var direction : net.minecraft.util.math.Direction.values()) {
                var at = pos.offset(direction);
                if (isLoaded(at) && mod.getWorld().getBlockState(at).isSolidBlock(mod.getWorld(), at)) support = true;
            }
            if (!support) { failure = "nothing_to_place_against"; return null; }
            return place;
        }
        @Override public boolean isFinished() {
            return failure != null || (isLoaded(pos) && mod.getWorld().getBlockState(pos).getBlock() == block);
        }
        @Override protected void onStop(Task interrupt) { }
        @Override protected boolean isEqual(Task other) { return this == other; }
        @Override protected String toDebugString() { return "IO place " + pos; }
    }

    private String digRefusal(BlockPos pos, String expected, boolean allowLiquid) {
        if (!isLoaded(pos)) return "not_loaded";
        var state = mod.getWorld().getBlockState(pos);
        if (state.isAir() || !state.getFluidState().isEmpty()) return null;
        if (!Registries.BLOCK.getId(state.getBlock()).getPath().equals(expected)) return "unexpected_block";
        var feet = mod.getPlayer().getBlockPos();
        if (pos.getX() == feet.getX() && pos.getZ() == feet.getZ()
                && (pos.getY() < feet.getY() || pos.getY() >= feet.getY() + 2)) return "unsafe_dig";
        for (var direction : net.minecraft.util.math.Direction.values()) {
            var neighbour = pos.offset(direction);
            if (!isLoaded(neighbour)) return "not_loaded";
            if (!allowLiquid && !mod.getWorld().getFluidState(neighbour).isEmpty()) return "liquid_adjacent";
        }
        if (state.getHardness(mod.getWorld(), pos) < 0) return "no_suitable_tool";
        if (state.isToolRequired()) {
            boolean suitable = false;
            for (int i = 0; i < mod.getPlayer().getInventory().size(); i++)
                if (mod.getPlayer().getInventory().getStack(i).isSuitableFor(state)) suitable = true;
            if (!suitable) return "no_suitable_tool";
        }
        return null;
    }

    private void startDig(String callId, JsonNode args) {
        BlockPos pos = readPosition(args.path("pos"));
        String expected = args.path("expectedBlock").asText();
        if (!expected.matches("[a-z0-9_]{1,128}")) throw new IllegalArgumentException("invalid expected block");
        var task = new IoDigTask(pos, expected, args.path("collect").asBoolean(true), args.path("allowLiquid").asBoolean(false));
        PartialSupplier observed = () -> {
            ObjectNode value = json.createObjectNode().put("block", expected);
            value.putObject("pos").put("x", pos.getX()).put("y", pos.getY()).put("z", pos.getZ());
            return value;
        };
        beginTask(callId, task, () -> result(callId, task.failure == null ? "ok" : "failed", task.failure, observed.get()), observed);
    }

    private final class IoDigTask extends Task implements ITaskRequiresGrounded {
        private final BlockPos pos;
        private final String expected;
        private final boolean collect;
        private final boolean allowLiquid;
        private final Task destroy;
        private String failure;
        private long brokenAt;
        IoDigTask(BlockPos pos, String expected, boolean collect, boolean allowLiquid) {
            this.pos = pos; this.expected = expected; this.collect = collect; this.allowLiquid = allowLiquid;
            destroy = new adris.altoclef.tasks.construction.DestroyBlockTask(pos);
        }
        private boolean gone() {
            return isLoaded(pos) && (mod.getWorld().getBlockState(pos).isAir() || !mod.getWorld().getFluidState(pos).isEmpty());
        }
        private java.util.Optional<net.minecraft.entity.ItemEntity> nearbyDrop() {
            return mod.getEntityTracker().getDroppedItems().stream()
                    .filter(entity -> entity.isAlive() && entity.squaredDistanceTo(net.minecraft.util.math.Vec3d.ofCenter(pos)) <= 16)
                    .min(java.util.Comparator.comparingDouble(entity -> entity.squaredDistanceTo(mod.getPlayer())));
        }
        @Override protected void onStart() { }
        @Override protected Task onTick() {
            failure = digRefusal(pos, expected, allowLiquid);
            if (failure != null) return null;
            if (!gone()) return destroy;
            if (brokenAt == 0) brokenAt = System.currentTimeMillis();
            if (collect && System.currentTimeMillis() - brokenAt >= 400)
                return nearbyDrop().map(entity -> (Task) new GetToEntityTask(entity, 0.5)).orElse(null);
            return null;
        }
        @Override public boolean isFinished() {
            return failure != null || (gone() && (!collect || (brokenAt != 0
                    && System.currentTimeMillis() - brokenAt >= 400 && (nearbyDrop().isEmpty() || System.currentTimeMillis() - brokenAt >= 8000))));
        }
        @Override protected void onStop(Task interrupt) { }
        @Override protected boolean isEqual(Task other) { return this == other; }
        @Override protected String toDebugString() { return "IO dig " + pos; }
    }

    private net.minecraft.entity.EquipmentSlot equipmentSlot(String slot) {
        return switch (slot) {
            case "hand" -> net.minecraft.entity.EquipmentSlot.MAINHAND;
            case "head" -> net.minecraft.entity.EquipmentSlot.HEAD;
            case "torso" -> net.minecraft.entity.EquipmentSlot.CHEST;
            case "legs" -> net.minecraft.entity.EquipmentSlot.LEGS;
            case "feet" -> net.minecraft.entity.EquipmentSlot.FEET;
            default -> throw new IllegalArgumentException("invalid equipment slot");
        };
    }

    private void startEquip(String callId, JsonNode args) {
        String name = args.path("item").asText();
        String slotName = args.path("slot").asText();
        var slot = equipmentSlot(slotName);
        var id = net.minecraft.util.Identifier.of("minecraft", name);
        if (!net.minecraft.registry.Registries.ITEM.containsId(id) || countOf(name) < 1) {
            result(callId, "failed", "nothing_to_equip", null); return;
        }
        var item = net.minecraft.registry.Registries.ITEM.get(id);
        if (slot != net.minecraft.entity.EquipmentSlot.MAINHAND
                && (!(item instanceof net.minecraft.item.ArmorItem armor) || armor.getSlotType() != slot)) {
            result(callId, "failed", "equip_failed", null); return;
        }
        var task = new IoEquipTask(item, slot);
        beginTask(callId, task, () -> result(callId, task.isFinished() ? "ok" : "failed",
                task.isFinished() ? null : "equip_failed", json.createObjectNode().put("slot", slotName).put("item", name)),
                () -> json.createObjectNode().put("slot", slotName).put("item", name));
    }

    private final class IoEquipTask extends Task {
        private final net.minecraft.item.Item item;
        private final net.minecraft.entity.EquipmentSlot slot;
        IoEquipTask(net.minecraft.item.Item item, net.minecraft.entity.EquipmentSlot slot) {
            this.item = item; this.slot = slot;
        }
        @Override protected void onStart() { }
        @Override protected Task onTick() {
            var cursor = adris.altoclef.util.helpers.StorageHelper.getItemStackInCursorSlot();
            if (!cursor.isEmpty() && (slot == net.minecraft.entity.EquipmentSlot.MAINHAND
                    || cursor.getItem() != item || mod.getPlayer().getEquippedStack(slot).getItem() == item)) {
                mod.getItemStorage().getSlotThatCanFitInPlayerInventory(cursor, false).ifPresent(destination ->
                        mod.getSlotHandler().clickSlot(destination, 0, net.minecraft.screen.slot.SlotActionType.PICKUP));
                return null;
            }
            if (!(mod.getPlayer().currentScreenHandler instanceof net.minecraft.screen.PlayerScreenHandler)) {
                adris.altoclef.util.helpers.StorageHelper.closeScreen(); return null;
            }
            if (slot == net.minecraft.entity.EquipmentSlot.MAINHAND) {
                var sources = mod.getItemStorage().getSlotsWithItemPlayerInventory(false, item);
                if (!sources.isEmpty() && mod.getSlotHandler().canDoSlotAction()) {
                    mod.getPlayer().getInventory().selectedSlot = 1;
                    mod.getSlotHandler().clickSlot(sources.get(0), 1, net.minecraft.screen.slot.SlotActionType.SWAP);
                }
                return null;
            }
            return new adris.altoclef.tasks.slot.MoveItemToSlotFromInventoryTask(new ItemTarget(item, 1),
                    adris.altoclef.util.slots.PlayerSlot.getEquipSlot(slot));
        }
        @Override public boolean isFinished() {
            return mod.getPlayer().getEquippedStack(slot).getItem() == item
                    && adris.altoclef.util.helpers.StorageHelper.getItemStackInCursorSlot().isEmpty();
        }
        @Override protected void onStop(Task interrupt) { }
        @Override protected boolean isEqual(Task other) { return this == other; }
        @Override protected String toDebugString() { return "IO equip " + item; }
    }

    private void startSmelt(String callId, JsonNode args) {
        String inputName = args.path("smeltInput").asText();
        var recipe = smeltingRecipe(inputName);
        if (recipe.isEmpty()) { result(callId, "failed", "nothing_smelted", null); return; }
        var output = recipe.get().value().getResult(mod.getWorld().getRegistryManager()).getItem();
        String outputName = net.minecraft.registry.Registries.ITEM.getId(output).getPath();
        if (!outputName.equals(args.path("item").asText())) throw new IllegalArgumentException("smelt product mismatch");
        var input = net.minecraft.registry.Registries.ITEM.get(net.minecraft.util.Identifier.of("minecraft", inputName));
        int count = args.path("count").asInt(1);
        if (count < 1 || count > 64) throw new IllegalArgumentException("invalid smelt count");
        if (countOf(inputName) < count) { result(callId, "failed", "no_input_item", null); return; }
        if (args.hasNonNull("fuel")) {
            String fuelName = args.path("fuel").asText();
            var id = net.minecraft.util.Identifier.of("minecraft", fuelName);
            if (!net.minecraft.registry.Registries.ITEM.containsId(id) || countOf(fuelName) < 1) {
                result(callId, "failed", "no_fuel", null); return;
            }
            var fuel = net.minecraft.registry.Registries.ITEM.get(id);
            if (!adris.altoclef.util.helpers.ItemHelper.isFuel(fuel)
                    || adris.altoclef.util.helpers.ItemHelper.getFuelAmount(fuel) * countOf(fuelName) < count) { result(callId, "failed", "no_fuel", null); return; }
            smeltFuel = fuel;
        }
        int before = countOf(outputName);
        var task = new adris.altoclef.tasks.container.SmeltInFurnaceTask(new adris.altoclef.util.SmeltTarget(
                new ItemTarget(output, before + count), new ItemTarget(input, before + count)));
        task.ignoreMaterials();
        PartialSupplier observed = () -> json.createObjectNode().put("input", inputName).put("output", outputName)
                .put("smelted", countOf(outputName) - before);
        beginTask(callId, task, () -> {
            smeltFuel = null;
            ObjectNode value = observed.get();
            result(callId, value.path("smelted").asInt() >= count ? "ok" : "failed", null, value);
        }, observed);
    }

    private void startGet(String callId, String item, int count, boolean additional) {
        if (!TaskCatalogue.taskExists(item)) {
            result(callId, "failed", "unknown_item", null);
            return;
        }
        if (count < 1 || count > 2304) throw new IllegalArgumentException("invalid item count");
        int before = countOf(item);
        int target = additional ? Math.addExact(before, count) : count;
        if (target > 2304) throw new IllegalArgumentException("target exceeds inventory capacity");
        Task task = TaskCatalogue.getItemTask(item, target);
        beginTask(callId, task, () -> {
            int have = countOf(item);
            ObjectNode value = json.createObjectNode();
            value.put("item", item);
            value.put("count", have);
            value.put("before", before);
            value.put("gained", have - before);
            value.put("usedTable", usedCraftingTable);
            if (have >= target) result(callId, "ok", null, value);
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

    /** Follow moving entities; optional arrival/duration bounds retain the skill contracts. */
    private void startFollow(String callId, JsonNode args) {
        String player = args.path("player").asText();
        double distance = args.path("distance").asDouble(2);
        boolean arrival = args.has("stopWithin");
        if (arrival) distance = args.path("stopWithin").asDouble();
        long duration = args.path("maxDurationMs").asLong(Long.MAX_VALUE);
        if (!player.matches("[A-Za-z0-9_]{1,16}") || !Double.isFinite(distance) || distance < 1 || distance > 16
                || duration < 1000 || (args.has("maxDurationMs") && duration > 3600000))
            throw new IllegalArgumentException("invalid follow arguments");
        if (mod.getEntityTracker().getPlayerEntity(player).isEmpty()) {
            result(callId, "failed", "player_not_visible", null); return;
        }
        BridgeFollowTask task = new BridgeFollowTask(player, distance, duration, arrival);
        PartialSupplier observed = () -> {
            ObjectNode value = playerDistance(player);
            value.put("followedForMs", Math.max(0, System.currentTimeMillis() - task.startedAt));
            return value;
        };
        beginTask(callId, task, () -> {
            ObjectNode value = observed.get();
            value.put("endedBecause", mod.getEntityTracker().getPlayerEntity(player).isEmpty()
                    ? "player_left" : task.arrival ? "arrived" : "duration_reached");
            result(callId, "ok", null, value);
        }, observed);
    }

    private final class BridgeFollowTask extends Task {
        private final String player;
        private final double distance;
        private final long duration;
        private final boolean arrival;
        private long startedAt;
        private BridgeFollowTask(String player, double distance, long duration, boolean arrival) {
            this.player = player; this.distance = distance; this.duration = duration; this.arrival = arrival;
        }
        @Override protected void onStart() { startedAt = System.currentTimeMillis(); }
        @Override protected Task onTick() {
            return mod.getEntityTracker().getPlayerEntity(player)
                    .map(entity -> (Task) new GetToEntityTask(entity, distance)).orElse(null);
        }
        @Override protected void onStop(Task interrupt) { }
        @Override public boolean isFinished() {
            if (startedAt == 0) return false;
            var target = mod.getEntityTracker().getPlayerEntity(player);
            return target.isEmpty() || (arrival && target.get().distanceTo(mod.getPlayer()) <= distance)
                    || System.currentTimeMillis() - startedAt >= duration;
        }
        @Override protected boolean isEqual(Task other) { return this == other; }
        @Override protected String toDebugString() { return "IO follow " + player; }
    }

    /** The result reports dropped items; the operator must independently verify pickup. */
    private void startGive(String callId, JsonNode args) {
        if (args.path("heldOnly").asBoolean(false)) { startDropHeld(callId, args); return; }
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

    /** IO's inventory-only drop route never gathers replacement gifts. */
    private void startDropHeld(String callId, JsonNode args) {
        String name = args.path("item").asText();
        String player = args.hasNonNull("player") ? args.path("player").asText() : null;
        int count = args.path("count").asInt();
        if (count < 1 || count > 2304 || (player != null && !player.matches("[A-Za-z0-9_]{1,16}")))
            throw new IllegalArgumentException("invalid drop arguments");
        var id = net.minecraft.util.Identifier.of("minecraft", name);
        if (!Registries.ITEM.containsId(id)) { result(callId, "failed", "unknown_item", null); return; }
        if (countOf(name) < count) { result(callId, "failed", "not_carrying", null); return; }
        int before = countOf(name);
        var task = new IoDropHeldTask(Registries.ITEM.get(id), count, player);
        PartialSupplier observed = () -> json.createObjectNode().put("item", name).put("dropped", task.dropped)
                .put("inventoryDecrease", before - countOf(name)).put("throwActions", task.actions);
        beginTask(callId, task, () -> result(callId,
                task.dropped == count ? "ok" : "failed", task.dropped == count ? null : "player_unreachable", observed.get()), observed);
    }

    private final class IoDropHeldTask extends Task {
        private final net.minecraft.item.Item item;
        private final int count;
        private final String player;
        private int dropped;
        private int actions;
        private boolean playerGone;
        IoDropHeldTask(net.minecraft.item.Item item, int count, String player) {
            this.item = item; this.count = count; this.player = player;
        }
        @Override protected void onStart() {
            mod.getBehaviour().push(); mod.getBehaviour().addProtectedItems(item);
        }
        @Override protected Task onTick() {
            if (player != null) {
                var target = mod.getEntityTracker().getPlayerEntity(player);
                if (target.isEmpty()) { playerGone = true; return null; }
                if (target.get().distanceTo(mod.getPlayer()) > 2)
                    return new GetToEntityTask(target.get(), 2);
                adris.altoclef.util.helpers.LookHelper.lookAt(mod, target.get().getPos().add(0, 1.2, 0));
            }
            var cursor = StorageHelper.getItemStackInCursorSlot();
            if (!cursor.isEmpty()) {
                mod.getItemStorage().getSlotThatCanFitInPlayerInventory(cursor, false).ifPresent(destination ->
                        mod.getSlotHandler().clickSlot(destination, 0, net.minecraft.screen.slot.SlotActionType.PICKUP));
                return null;
            }
            if (!(mod.getPlayer().currentScreenHandler instanceof net.minecraft.screen.PlayerScreenHandler)) {
                StorageHelper.closeScreen(); return null;
            }
            var sources = mod.getItemStorage().getSlotsWithItemPlayerInventory(false, item);
            if (!sources.isEmpty() && mod.getSlotHandler().canDoSlotAction()) {
                var source = sources.stream()
                        .filter(candidate -> StorageHelper.getItemStackInSlot(candidate).getCount() <= count - dropped)
                        .max(java.util.Comparator.comparingInt(candidate -> StorageHelper.getItemStackInSlot(candidate).getCount()))
                        .orElse(sources.get(0));
                int before = StorageHelper.getItemStackInSlot(source).getCount();
                // A whole inventory slot goes in one throw only when all of it is requested.
                int button = before <= count - dropped ? 1 : 0;
                mod.getSlotHandler().clickSlot(source, button, net.minecraft.screen.slot.SlotActionType.THROW);
                dropped += before - StorageHelper.getItemStackInSlot(source).getCount();
                actions++;
            }
            return null;
        }
        @Override public boolean isFinished() { return dropped >= count || playerGone; }
        @Override protected void onStop(Task interrupt) { mod.getBehaviour().pop(); }
        @Override protected boolean isEqual(Task other) { return this == other; }
        @Override protected String toDebugString() { return "IO drop held " + item; }
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

    private BlockPos readPosition(JsonNode node) {
        for (String axis : new String[]{"x", "y", "z"}) {
            if (!node.path(axis).isIntegralNumber()) throw new IllegalArgumentException("integer position required");
        }
        BlockPos pos = new BlockPos(node.path("x").asInt(), node.path("y").asInt(), node.path("z").asInt());
        if (Math.abs((long) pos.getX()) > 29999984 || Math.abs((long) pos.getZ()) > 29999984
                || pos.getY() < mod.getWorld().getBottomY() || pos.getY() >= mod.getWorld().getTopY()) {
            throw new IllegalArgumentException("position outside world");
        }
        return pos;
    }

    private void startClearArea(String callId, JsonNode args) {
        BlockPos a = readPosition(args.path("from"));
        BlockPos b = readPosition(args.path("to"));
        BlockPos from = new BlockPos(Math.min(a.getX(), b.getX()), Math.min(a.getY(), b.getY()), Math.min(a.getZ(), b.getZ()));
        BlockPos to = new BlockPos(Math.max(a.getX(), b.getX()), Math.max(a.getY(), b.getY()), Math.max(a.getZ(), b.getZ()));
        startBuilder(callId, from, to, null);
    }

    private void startBuild(String callId, JsonNode args) throws Exception {
        String name = args.path("schematic").asText();
        if (!name.matches("[A-Za-z0-9_-]+\\.schem")) throw new IllegalArgumentException("invalid schematic name");
        Path directory = MinecraftClient.getInstance().runDirectory.toPath().resolve("schematics").toRealPath();
        Path file = directory.resolve(name).toRealPath();
        if (!file.getParent().equals(directory)) throw new IllegalArgumentException("schematic outside directory");
        var format = BaritoneAPI.getProvider().getSchematicSystem().getByFile(file.toFile())
                .orElseThrow(() -> new IllegalArgumentException("unsupported schematic"));
        IStaticSchematic schematic;
        try (FileInputStream stream = new FileInputStream(file.toFile())) {
            schematic = format.parse(stream);
        }
        BlockPos origin = readPosition(args.path("origin"));
        if (schematic.widthX() <= 0 || schematic.heightY() <= 0 || schematic.lengthZ() <= 0) {
            throw new IllegalArgumentException("empty schematic");
        }
        BlockPos to = origin.add(schematic.widthX() - 1, schematic.heightY() - 1, schematic.lengthZ() - 1);
        readPosition(json.createObjectNode().put("x", to.getX()).put("y", to.getY()).put("z", to.getZ()));
        startBuilder(callId, origin, to, schematic);
    }

    private void startBuilder(String callId, BlockPos from, BlockPos to, IStaticSchematic schematic) {
        long volume = ((long) to.getX() - from.getX() + 1) * ((long) to.getY() - from.getY() + 1)
                * ((long) to.getZ() - from.getZ() + 1);
        if (volume <= 0 || volume > 32768) throw new IllegalArgumentException("region exceeds 32768 blocks");
        BridgeBuilderTask task = new BridgeBuilderTask(from, to, schematic);
        PartialSupplier partial = () -> json.createObjectNode().put("mismatches", task.mismatches())
                .put("paused", mod.getClientBaritone().getBuilderProcess().isPaused());
        beginTask(callId, task, () -> result(callId, task.mismatches() == 0 ? "ok" : "failed",
                task.mismatches() == 0 ? null : "builder_incomplete", partial.get()), partial);
    }

    /** Completion is checked against the loaded world, including both endpoints of the region. */
    private final class BridgeBuilderTask extends Task implements ITaskRequiresGrounded {
        private final BlockPos from;
        private final BlockPos to;
        private final IStaticSchematic schematic;
        private boolean started;
        private boolean previousBuildInLayers;
        private boolean previousLayerOrder;
        private int previousLayerHeight;

        private BridgeBuilderTask(BlockPos from, BlockPos to, IStaticSchematic schematic) {
            this.from = from;
            this.to = to;
            this.schematic = schematic;
        }

        private int mismatches() {
            int count = 0;
            for (BlockPos pos : BlockPos.iterate(from, to)) {
                if (!isLoaded(pos)) { count++; continue; }
                var actual = mod.getWorld().getBlockState(pos);
                if (schematic == null ? !actual.isAir() : !actual.equals(schematic.getDirect(
                        pos.getX() - from.getX(), pos.getY() - from.getY(), pos.getZ() - from.getZ()))) count++;
            }
            return count;
        }

        @Override protected void onStart() {
            started = true;
            if (schematic == null) {
                var settings = BaritoneAPI.getSettings();
                previousBuildInLayers = settings.buildInLayers.value;
                previousLayerOrder = settings.layerOrder.value;
                previousLayerHeight = settings.layerHeight.value;
                // Preserve lower ground and scaffolds until all reachable upper blocks are gone.
                settings.buildInLayers.value = true;
                settings.layerOrder.value = true;
                settings.layerHeight.value = 4;
                mod.getClientBaritone().getBuilderProcess().clearArea(from, to);
            } else mod.getClientBaritone().getBuilderProcess().build("IO schematic", schematic, from);
        }
        @Override protected Task onTick() { return null; }
        @Override protected void onStop(Task interrupt) {
            mod.getClientBaritone().getBuilderProcess().onLostControl();
            if (schematic == null && started) {
                var settings = BaritoneAPI.getSettings();
                settings.buildInLayers.value = previousBuildInLayers;
                settings.layerOrder.value = previousLayerOrder;
                settings.layerHeight.value = previousLayerHeight;
            }
        }
        @Override public boolean isFinished() { return started && mismatches() == 0; }
        @Override protected boolean isEqual(Task other) { return this == other; }
        @Override protected String toDebugString() { return "IO builder " + from + " to " + to; }
    }

    private interface PartialSupplier {
        ObjectNode get();
    }

    private PartialSupplier currentPartial = () -> json.createObjectNode();

    private void beginTask(String callId, Task task, Runnable onDone, PartialSupplier partialNow) {
        runningCallId = callId;
        cancelling = false;
        usedCraftingTable = false;
        currentPartial = partialNow;
        mod.runUserTask(task, () -> {
            // The user chain also calls this when it is cancelled: then onCancel answers.
            if (cancelling) return;
            if (!callId.equals(runningCallId)) return;
            runningCallId = null;
            // Publish the postcondition state before settling Core's call.
            pushState(true);
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
            smeltFuel = null;
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
        if (runningCallId != null && mod.getPlayer() != null
                && mod.getPlayer().currentScreenHandler instanceof net.minecraft.screen.CraftingScreenHandler)
            usedCraftingTable = true;
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
        ObjectNode equipment = p.putObject("equipment");
        if (player != null) for (String slot : new String[]{"hand", "head", "torso", "legs", "feet"}) {
            ItemStack stack = player.getEquippedStack(equipmentSlot(slot));
            equipment.put(slot, stack.isEmpty() ? null : Registries.ITEM.getId(stack.getItem()).getPath());
        }
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
