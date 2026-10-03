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
    private static net.minecraft.screen.ScreenHandler syncedInventoryHandler;
    public static void receivedInventory(int syncId) {
        var player = MinecraftClient.getInstance().player;
        if (player != null && player.currentScreenHandler.syncId == syncId) syncedInventoryHandler = player.currentScreenHandler;
    }
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
                    if (args.path("droppedOnly").asBoolean(false)) startPickup(callId, args);
                    else if (args.has("smeltInput")) startSmelt(callId, args);
                    else startGet(callId, args.path("item").asText(), args.path("count").asInt(1), args.path("additional").asBoolean(false));
                }
                case "task.goto" -> startGoto(callId, args);
                case "prim.equip" -> startEquip(callId, args);
                case "prim.dig" -> startDig(callId, args);
                case "prim.place" -> startPlace(callId, args);
                case "prim.use" -> {
                    if ("use_on".equals(args.path("action").asText())) startUseOn(callId,args);
                    else startEat(callId,args);
                }
                case "prim.sleep" -> startSleep(callId,args);
                case "prim.activate" -> {
                    if ("escape_liquid".equals(args.path("interaction").asText())) startEscapeLiquid(callId,args);
                    else if ("clear_furnace".equals(args.path("interaction").asText())) startClearFurnace(callId,args);
                    else if ("use_door".equals(args.path("interaction").asText())) startDoor(callId,args);
                    else if ("withdraw".equals(args.path("interaction").asText())) startContainerTransfer(callId, args, false);
                    else startObserveContainer(callId, args);
                }
                case "task.deposit" -> startContainerTransfer(callId, args, true);
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
            var food=adris.altoclef.multiversion.item.ItemVer.getFoodComponent(Registries.ITEM.get(net.minecraft.util.Identifier.of("minecraft",item)));
            value.put("foodPoints",food==null ? 0 : food.getHunger());
            var recipe = smeltingRecipe(item);
            if (recipe.isPresent()) value.put("smeltOutput", net.minecraft.registry.Registries.ITEM.getId(
                    recipe.get().value().getResult(mod.getWorld().getRegistryManager()).getItem()).getPath());
            else value.putNull("smeltOutput");
        } else if ("query.blockAt".equals(op)) {
            BlockPos pos = readPosition(args.path("pos"));
            boolean loaded = isLoaded(pos);
            value.put("loaded", loaded);
            if (loaded) {
                var state=mod.getWorld().getBlockState(pos);
                value.put("name", Registries.BLOCK.getId(state.getBlock()).getPath());
                ObjectNode properties=value.putObject("properties");
                state.getEntries().forEach((property,setting)->properties.put(property.getName(),setting instanceof net.minecraft.util.StringIdentifiable named ? named.asString() : setting.toString()));
            }
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
        } else if ("query.findBlocks".equals(op)) {
            int radius = args.path("radius").asInt(16), limit = args.path("limit").asInt(1);
            if (radius < 1 || radius > 64 || limit < 1 || limit > 64 || !args.path("blocks").isArray()
                    || args.path("blocks").size() < 1 || args.path("blocks").size() > 32)
                throw new IllegalArgumentException("invalid bounded block search");
            java.util.Set<net.minecraft.block.Block> wanted = new java.util.HashSet<>();
            for (JsonNode name : args.path("blocks")) {
                var id = net.minecraft.util.Identifier.of("minecraft", name.asText());
                if (Registries.BLOCK.containsId(id)) wanted.add(Registries.BLOCK.get(id));
            }
            var center = args.has("near") ? readPosition(args.path("near")) : mod.getPlayer().getBlockPos();
            var hits = new java.util.PriorityQueue<BlockPos>(java.util.Comparator.<BlockPos>comparingDouble(pos -> pos.getSquaredDistance(center)).reversed());
            var loadedChunks = new java.util.HashMap<Long, Boolean>();
            for (BlockPos pos : BlockPos.iterate(center.add(-radius,-radius,-radius), center.add(radius,radius,radius))) {
                if (pos.getSquaredDistance(center) > radius * radius) continue;
                long chunk = net.minecraft.util.math.ChunkPos.toLong(pos.getX() >> 4, pos.getZ() >> 4);
                if (!loadedChunks.computeIfAbsent(chunk, key -> isLoaded(pos))) continue;
                if (wanted.contains(mod.getWorld().getBlockState(pos).getBlock())
                        && (!args.path("sourceFluidOnly").asBoolean(false) || mod.getWorld().getFluidState(pos).isStill())) {
                    hits.add(pos.toImmutable()); if (hits.size() > limit) hits.poll();
                }
            }
            ArrayNode found = value.putArray("blocks");
            hits.stream().sorted(java.util.Comparator.comparingDouble(pos -> pos.getSquaredDistance(center))).forEach(pos -> {
                ObjectNode row = found.addObject().put("name", Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath());
                row.putObject("pos").put("x",pos.getX()).put("y",pos.getY()).put("z",pos.getZ());
            });
        } else if ("query.light".equals(op)) {
            value.put("timeOfDay",Math.floorMod(mod.getWorld().getTimeOfDay(),24000));
            value.put("thundering",mod.getWorld().isThundering());
            value.put("bedsWork",mod.getWorld().getDimension().bedWorks());
            BlockPos pos=args.has("pos") ? readPosition(args.path("pos")) : mod.getPlayer().getBlockPos();
            value.put("loaded",isLoaded(pos));
            if (isLoaded(pos)) {
                value.put("blockLight",mod.getWorld().getLightLevel(net.minecraft.world.LightType.BLOCK,pos));
                value.put("skyLight",mod.getWorld().getLightLevel(net.minecraft.world.LightType.SKY,pos));
            }
        } else if ("query.container".equals(op)) {
            var expected=args.has("pos") ? readPosition(args.path("pos")) : null;
            var observed = "furnace".equals(args.path("kind").asText()) ? screenSnapshot(expected,true) : containerSnapshot(expected);
            if (observed == null) { result(callId,"failed","container_not_open",null); return; }
            value = observed;
        } else if ("query.entities".equals(op)) {
            ArrayNode entities = value.putArray("entities");
            for (var entity : mod.getWorld().getEntities()) {
                ObjectNode entry = entities.addObject();
                entry.put("id", String.valueOf(entity.getId()));
                entry.put("uuid",entity.getUuid().toString()); entry.put("alive",entity.isAlive());
                if (entity instanceof net.minecraft.entity.passive.SheepEntity sheep) entry.put("sheared",sheep.isSheared());
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

    private boolean storageContainer(String name) {
        return java.util.Set.of("chest","trapped_chest","ender_chest","barrel","shulker_box").contains(name)
                || name.endsWith("_shulker_box");
    }

    private boolean furnace(String name) { return java.util.Set.of("furnace","blast_furnace","smoker").contains(name); }
    private ObjectNode containerSnapshot(BlockPos expected) { return screenSnapshot(expected,false); }
    private ObjectNode screenSnapshot(BlockPos expected,boolean furnace) {
        var handler = mod.getPlayer().currentScreenHandler;
        if (furnace) { if (!(handler instanceof net.minecraft.screen.AbstractFurnaceScreenHandler)) return null; }
        else if (!(handler instanceof net.minecraft.screen.GenericContainerScreenHandler)
                && !(handler instanceof net.minecraft.screen.ShulkerBoxScreenHandler)) return null;
        if (syncedInventoryHandler != handler) return null;
        var last = mod.getItemStorage().getLastBlockPosInteraction();
        if (last.isEmpty() || (expected != null && !last.get().equals(expected))) return null;
        BlockPos pos = last.get();
        if (!isLoaded(pos)) return null;
        String name = Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath();
        if (furnace ? !furnace(name) : !storageContainer(name)) return null;
        int slots = handler.slots.size() - 36, used = 0;
        ObjectNode value = json.createObjectNode().put("container", name).put("dimension", mod.getWorld().getRegistryKey().getValue().toString());
        value.putObject("position").put("x",pos.getX()).put("y",pos.getY()).put("z",pos.getZ());
        ArrayNode contents = value.putArray("contents");
        for (int i = 0; i < slots; i++) {
            ItemStack stack = handler.getSlot(i).getStack();
            if (!stack.isEmpty()) {
                used++;
                contents.addObject().put("slot",i).put("name",Registries.ITEM.getId(stack.getItem()).getPath()).put("count",stack.getCount());
            }
        }
        value.put("emptySlots", slots-used);
        return value;
    }

    private String liquidAtPlayer() {
        BlockPos feet=mod.getPlayer().getBlockPos();
        for (var pos:new BlockPos[]{feet,feet.up()}) {
            if (!isLoaded(pos)) return "unknown";
            var block=mod.getWorld().getBlockState(pos).getBlock();
            if (block==Blocks.WATER) return "water";
            if (block==Blocks.LAVA) return "lava";
        }
        return null;
    }
    private boolean dryStand(BlockPos pos) {
        if (!isLoaded(pos) || !isLoaded(pos.down()) || !isLoaded(pos.up())) return false;
        var ground=mod.getWorld().getBlockState(pos.down());
        if (!ground.isSolidBlock(mod.getWorld(),pos.down()) || ground.isOf(Blocks.MAGMA_BLOCK)) return false;
        for (var p:new BlockPos[]{pos,pos.up()}) {
            var state=mod.getWorld().getBlockState(p);
            if (!state.getCollisionShape(mod.getWorld(),p).isEmpty() || !state.getFluidState().isEmpty()
                    || state.isOf(Blocks.FIRE) || state.isOf(Blocks.SOUL_FIRE)) return false;
        }
        return true;
    }
    private void startEscapeLiquid(String callId,JsonNode args) {
        int radius=args.path("searchRadius").asInt(8);
        if (radius<2 || radius>32) throw new IllegalArgumentException("invalid escape radius");
        String liquid=liquidAtPlayer();
        if (liquid==null) { result(callId,"failed","not_in_liquid",null); return; }
        if (liquid.equals("unknown")) { result(callId,"failed","body_observation_unavailable",null); return; }
        var origin=mod.getPlayer().getBlockPos(); BlockPos exit=null;
        search: for (int r=1;r<=radius;r++) for (int dx=-r;dx<=r;dx++) for (int dz=-r;dz<=r;dz++) {
            if (Math.max(Math.abs(dx),Math.abs(dz))!=r) continue;
            for (int dy=-2;dy<=3;dy++) {
                var pos=origin.add(dx,dy,dz);
                if (dryStand(pos)) { exit=pos; break search; }
            }
        }
        if (exit==null) { result(callId,"failed","no_safe_ground",null); return; }
        var task=new IoEscapeLiquidTask(liquid,exit);
        beginTask(callId,task,()->result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
    }
    private final class IoEscapeLiquidTask extends Task {
        private final String liquid;
        private final BlockPos exit;
        private long started;
        private boolean done,oldBreak,oldPlace,oldParkour;
        private int oldFall;
        private String failure;
        IoEscapeLiquidTask(String liquid,BlockPos exit) { this.liquid=liquid; this.exit=exit; }
        ObjectNode value() {
            var value=json.createObjectNode().put("liquid",liquid);
            value.putObject("escapedTo").put("x",exit.getX()).put("y",exit.getY()).put("z",exit.getZ());
            return value;
        }
        @Override protected void onStart() {
            started=System.currentTimeMillis(); mod.getBehaviour().push(); mod.getBehaviour().avoidBlockBreaking(p->true);
            var settings=mod.getClientBaritoneSettings(); oldBreak=settings.allowBreak.value; oldPlace=settings.allowPlace.value;
            oldFall=settings.maxFallHeightNoWater.value; oldParkour=settings.allowParkour.value;
            settings.allowBreak.value=false; settings.allowPlace.value=false; settings.maxFallHeightNoWater.value=3; settings.allowParkour.value=false;
            mod.getClientBaritone().getPathingBehavior().forceCancel();
        }
        @Override protected Task onTick() {
            if (liquidAtPlayer()==null && mod.getPlayer().squaredDistanceTo(net.minecraft.util.math.Vec3d.ofBottomCenter(exit))<4) { done=true; return null; }
            if (!dryStand(exit)) { failure="no_safe_ground"; done=true; return null; }
            if (System.currentTimeMillis()-started>30000) { failure="still_in_liquid"; done=true; return null; }
            mod.getInputControls().hold(baritone.api.utils.input.Input.JUMP);
            if (!mod.getClientBaritone().getCustomGoalProcess().isActive())
                mod.getClientBaritone().getCustomGoalProcess().setGoalAndPath(new baritone.api.pathing.goals.GoalBlock(exit));
            return null;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) {
            mod.getInputControls().release(baritone.api.utils.input.Input.JUMP); mod.getClientBaritone().getPathingBehavior().forceCancel();
            mod.getBehaviour().pop(); var settings=mod.getClientBaritoneSettings();
            settings.allowBreak.value=oldBreak; settings.allowPlace.value=oldPlace; settings.maxFallHeightNoWater.value=oldFall; settings.allowParkour.value=oldParkour;
        }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO escape " + liquid + " onto dry ground"; }
    }

    private void startUseOn(String callId,JsonNode args) {
        String item=args.path("item").asText();
        var id=net.minecraft.util.Identifier.of("minecraft",item);
        if (!Registries.ITEM.containsId(id) || countOf(item)<1) { result(callId,"failed","not_in_inventory",null); return; }
        var target=args.path("target");
        BlockPos pos=target.has("pos") ? readPosition(target.path("pos")) : null;
        int entityId=target.path("entityId").asInt(-1);
        UUID entityUuid=target.has("entityUuid") ? UUID.fromString(target.path("entityUuid").asText()) : null;
        if (pos==null && (entityId<0 || entityUuid==null)) throw new IllegalArgumentException("entity identity required");
        if (pos!=null && !target.has("expectedBlock")) throw new IllegalArgumentException("block identity required");
        var task=new IoUseOnTask(item,Registries.ITEM.get(id),pos,target.path("expectedBlock").asText(),entityId,entityUuid);
        beginTask(callId,task,() -> result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
    }
    private final class IoUseOnTask extends Task implements ITaskRequiresGrounded {
        private final String itemName,expectedBlock;
        private final net.minecraft.item.Item item;
        private final BlockPos pos;
        private final int entityId;
        private final UUID entityUuid;
        private net.minecraft.util.math.Vec3d at;
        private String expected,failure;
        private int before;
        private long started,usedAt;
        private boolean done,oldBreak,oldPlace;
        private Task movement;
        IoUseOnTask(String itemName,net.minecraft.item.Item item,BlockPos pos,String expectedBlock,int entityId,UUID entityUuid) {
            this.itemName=itemName; this.item=item; this.pos=pos; this.expectedBlock=expectedBlock; this.entityId=entityId; this.entityUuid=entityUuid;
        }
        private int quantity(String expected) {
            if (!"wool".equals(expected)) return countOf(expected);
            int total=0;
            for (int i=0;i<36;i++) {
                ItemStack stack=mod.getPlayer().getInventory().getStack(i);
                if (Registries.ITEM.getId(stack.getItem()).getPath().endsWith("_wool")) total+=stack.getCount();
            }
            return total;
        }
        ObjectNode value() {
            ObjectNode value=json.createObjectNode().put("verified",expected!=null).put("before",before);
            if (expected!=null) value.put("expected",expected);
            if (at!=null) value.putObject("at").put("x",pos==null ? at.x : pos.getX()).put("y",pos==null ? at.y : pos.getY()).put("z",pos==null ? at.z : pos.getZ());
            return value;
        }
        @Override protected void onStart() {
            started=System.currentTimeMillis(); StorageHelper.closeScreen();
            var settings=mod.getClientBaritoneSettings(); oldBreak=settings.allowBreak.value; oldPlace=settings.allowPlace.value;
            mod.getBehaviour().push(); mod.getBehaviour().addProtectedItems(item); mod.getBehaviour().avoidBlockBreaking(p->true);
            settings.allowBreak.value=false; settings.allowPlace.value=false;
        }
        @Override protected Task onTick() {
            long now=System.currentTimeMillis();
            if (usedAt!=0) {
                if (expected!=null && quantity(expected)>before) { done=true; return null; }
                if (expected==null && now-usedAt>=400) { done=true; return null; }
                if (now-usedAt>4000) { failure="item_use_failed"; done=true; return null; }
                if ("wool".equals(expected) && now-usedAt>=500) {
                    var drop=mod.getEntityTracker().getDroppedItems().stream().filter(e->e.isAlive()
                        && e.squaredDistanceTo(at)<=16 && Registries.ITEM.getId(e.getStack().getItem()).getPath().endsWith("_wool"))
                        .min(java.util.Comparator.comparingDouble(e->e.squaredDistanceTo(mod.getPlayer())));
                    if (drop.isPresent()) return new GetToEntityTask(drop.get(),0.25);
                }
                return null;
            }
            var entity=pos==null ? mod.getWorld().getEntityById(entityId) : null;
            if (pos==null && (entity==null || !entity.isAlive() || !entity.getUuid().equals(entityUuid))) { failure="target_not_found"; done=true; return null; }
            if (pos!=null && (!isLoaded(pos) || !Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath().equals(expectedBlock))) { failure="target_not_found"; done=true; return null; }
            at=entity!=null ? entity.getPos() : net.minecraft.util.math.Vec3d.ofCenter(pos);
            if (now-started>30000) { failure="container_unreachable"; done=true; return null; }
            if (mod.getPlayer().squaredDistanceTo(at)>9) {
                if (entity!=null) return new GetToEntityTask(entity,2);
                if (movement==null) movement=new adris.altoclef.tasks.movement.CustomBaritoneGoalTask(false) {
                    @Override protected baritone.api.pathing.goals.Goal newGoal(AltoClef mod) { return new baritone.api.pathing.goals.GoalNear(pos,2); }
                    @Override protected boolean isEqual(Task other) { return this==other; }
                    @Override protected String toDebugString() { return "IO approach item-use target"; }
                };
                return movement;
            }
            if (countOf(itemName)<1) { failure="not_in_inventory"; done=true; return null; }
            if (itemName.equals("bucket") && pos!=null && mod.getWorld().getBlockState(pos).isOf(Blocks.WATER)) {
                if (!mod.getWorld().getFluidState(pos).isStill()) { failure="not_water_source"; done=true; return null; }
                expected="water_bucket";
            } else if (itemName.equals("shears") && entity instanceof net.minecraft.entity.passive.SheepEntity sheep) {
                if (!sheep.isShearable() || sheep.isSheared()) { failure="item_use_failed"; done=true; return null; }
                var wool=new ItemStack(Registries.ITEM.get(net.minecraft.util.Identifier.of("minecraft",sheep.getColor().getName()+"_wool")));
                boolean room=false;
                for (int i=0;i<36;i++) {
                    var held=mod.getPlayer().getInventory().getStack(i);
                    if (held.isEmpty() || (ItemStack.areItemsAndComponentsEqual(held,wool) && held.getCount()<=held.getMaxCount()-3)) { room=true; break; }
                }
                if (!room) { failure="inventory_full"; done=true; return null; }
                expected="wool";
            }
            before=expected==null ? 0 : quantity(expected);
            if (!mod.getSlotHandler().forceEquipItem(new net.minecraft.item.Item[]{item},true)) return null;
            var look=entity!=null ? at.add(0,entity.getHeight()/2,0) : at;
            adris.altoclef.util.helpers.LookHelper.lookAt(mod,look,false);
            if (entity!=null) mod.getController().interactEntity(mod.getPlayer(),entity,net.minecraft.util.Hand.MAIN_HAND);
            else if (itemName.contains("bucket")) mod.getController().interactItem(mod.getPlayer(),net.minecraft.util.Hand.MAIN_HAND);
            else mod.getController().interactBlock(mod.getPlayer(),net.minecraft.util.Hand.MAIN_HAND,
                new net.minecraft.util.hit.BlockHitResult(at,net.minecraft.util.math.Direction.UP,pos,false));
            usedAt=now; return null;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) {
            mod.getClientBaritone().getPathingBehavior().forceCancel(); mod.getInputControls().release(baritone.api.utils.input.Input.CLICK_RIGHT);
            StorageHelper.closeScreen(); mod.getBehaviour().pop();
            mod.getClientBaritoneSettings().allowBreak.value=oldBreak; mod.getClientBaritoneSettings().allowPlace.value=oldPlace;
        }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO use held " + itemName + " on observed target"; }
    }

    private void startClearFurnace(String callId,JsonNode args) {
        BlockPos pos=readPosition(args.path("pos"));
        if (!isLoaded(pos)) { result(callId,"failed","no_furnace",null); return; }
        String name=Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath();
        if (!furnace(name)) { result(callId,"failed","no_furnace",null); return; }
        var task=new IoClearFurnaceTask(pos,name);
        beginTask(callId,task,() -> result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
    }
    private final class IoClearFurnaceTask extends Task implements ITaskRequiresGrounded {
        private final BlockPos pos;
        private final String name;
        private final Task interact;
        private final java.util.Map<String,Integer> gains=new java.util.HashMap<>();
        private net.minecraft.screen.ScreenHandler handler;
        private ObjectNode observed;
        private String pending,failure;
        private int pendingBefore,taken;
        private long clicked;
        private boolean done;
        IoClearFurnaceTask(BlockPos pos,String name) { this.pos=pos; this.name=name; interact=new adris.altoclef.tasks.InteractWithBlockTask(pos); }
        ObjectNode value() {
            int extra=pending==null ? 0 : Math.max(0,countOf(pending)-pendingBefore);
            ObjectNode value=json.createObjectNode().put("taken",taken+extra);
            value.putObject("at").put("x",pos.getX()).put("y",pos.getY()).put("z",pos.getZ());
            ObjectNode amounts=value.putObject("gains"); gains.forEach(amounts::put);
            if (extra>0) amounts.put(pending,gains.getOrDefault(pending,0)+extra);
            if (observed!=null) value.set("observation",observed.deepCopy());
            return value;
        }
        @Override protected void onStart() { StorageHelper.closeScreen(); syncedInventoryHandler=null; mod.getBehaviour().push(); }
        @Override protected Task onTick() {
            if (!isLoaded(pos) || !Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath().equals(name)) {
                failure="no_furnace"; done=true; return null;
            }
            if (handler==null) {
                if (screenSnapshot(pos,true)==null) return interact;
                handler=mod.getPlayer().currentScreenHandler;
            }
            if (mod.getPlayer().currentScreenHandler!=handler) { failure="furnace_not_empty"; done=true; return null; }
            if (pending!=null) {
                if (System.currentTimeMillis()-clicked<400) return null;
                int gained=countOf(pending)-pendingBefore;
                if (gained<=0) { failure="furnace_not_empty"; observed=screenSnapshot(pos,true); done=true; return null; }
                gains.merge(pending,gained,Integer::sum); taken+=gained; pending=null;
            }
            if (!mod.getSlotHandler().canDoSlotAction()) return null;
            for (int i=0;i<3;i++) {
                ItemStack stack=handler.getSlot(i).getStack();
                if (stack.isEmpty()) continue;
                mod.getBehaviour().addProtectedItems(stack.getItem());
                pending=Registries.ITEM.getId(stack.getItem()).getPath(); pendingBefore=countOf(pending);
                mod.getController().clickSlot(handler.syncId,i,0,net.minecraft.screen.slot.SlotActionType.QUICK_MOVE,mod.getPlayer());
                mod.getSlotHandler().registerSlotAction(); clicked=System.currentTimeMillis(); return null;
            }
            observed=screenSnapshot(pos,true); done=true; return null;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) { StorageHelper.closeScreen(); mod.getBehaviour().pop(); }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO recover furnace contents " + pos; }
    }

    private void startDoor(String callId, JsonNode args) {
        BlockPos pos=readPosition(args.path("pos"));
        if (!isLoaded(pos) || !(mod.getWorld().getBlockState(pos).getBlock() instanceof net.minecraft.block.DoorBlock)) {
            result(callId,"failed","not_a_door",null); return;
        }
        var state=mod.getWorld().getBlockState(pos);
        if (state.get(net.minecraft.block.DoorBlock.HALF)==net.minecraft.block.enums.DoubleBlockHalf.UPPER) pos=pos.down();
        if (!isLoaded(pos) || !(mod.getWorld().getBlockState(pos).getBlock() instanceof net.minecraft.block.DoorBlock)) {
            result(callId,"failed","not_a_door",null); return;
        }
        String name=Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath();
        if (name.equals("iron_door")) { result(callId,"failed","needs_redstone",null); return; }
        var task=new IoDoorTask(pos,name,args.path("close").asBoolean(true));
        beginTask(callId,task,() -> result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
    }

    private final class IoDoorTask extends Task implements ITaskRequiresGrounded {
        private final BlockPos pos;
        private final String name,axis;
        private final boolean close;
        private int side,phase;
        private BlockPos near,far;
        private Task movement;
        private long phaseStarted,lastClick;
        private boolean oldBreak,oldPlace,done,flipped;
        private String failure;
        IoDoorTask(BlockPos pos,String name,boolean close) {
            this.pos=pos; this.name=name; this.close=close;
            var facing=mod.getWorld().getBlockState(pos).get(net.minecraft.block.DoorBlock.FACING);
            axis=facing.getAxis()==net.minecraft.util.math.Direction.Axis.Z ? "z" : "x";
            side=(axis.equals("z") ? mod.getPlayer().getZ()<pos.getZ()+0.5 : mod.getPlayer().getX()<pos.getX()+0.5) ? -1 : 1;
            near=offset(side); far=offset(-side); movement=new GetToBlockTask(near);
        }
        private BlockPos offset(int side) { return axis.equals("z") ? pos.add(0,0,side) : pos.add(side,0,0); }
        private int sideNow() { return (axis.equals("z") ? mod.getPlayer().getZ()<pos.getZ()+0.5 : mod.getPlayer().getX()<pos.getX()+0.5) ? -1 : 1; }
        private boolean reached(BlockPos target) {
            return Math.hypot(mod.getPlayer().getX()-target.getX()-0.5,mod.getPlayer().getZ()-target.getZ()-0.5)<0.7
                && Math.abs(mod.getPlayer().getY()-target.getY())<0.5;
        }
        private boolean standable(BlockPos target) {
            return isLoaded(target) && isLoaded(target.up()) && isLoaded(target.down())
                && mod.getWorld().getBlockState(target).getCollisionShape(mod.getWorld(),target).isEmpty()
                && mod.getWorld().getBlockState(target.up()).getCollisionShape(mod.getWorld(),target.up()).isEmpty()
                && mod.getWorld().getBlockState(target.down()).isSolidBlock(mod.getWorld(),target.down());
        }
        private boolean setOpen(boolean open) {
            if (mod.getWorld().getBlockState(pos).get(net.minecraft.block.DoorBlock.OPEN)==open) return true;
            if (System.currentTimeMillis()-lastClick>=500 && mod.getPlayer().squaredDistanceTo(net.minecraft.util.math.Vec3d.ofCenter(pos))<=20.25) {
                mod.getController().interactBlock(mod.getPlayer(),net.minecraft.util.Hand.MAIN_HAND,
                    new net.minecraft.util.hit.BlockHitResult(net.minecraft.util.math.Vec3d.ofCenter(pos),net.minecraft.util.math.Direction.UP,pos,false));
                lastClick=System.currentTimeMillis();
            }
            return false;
        }
        private void next(int phase) { this.phase=phase; phaseStarted=System.currentTimeMillis(); }
        ObjectNode value() {
            ObjectNode value=json.createObjectNode().put("axis",axis).put("from",side).put("to",sideNow());
            value.putObject("door").put("x",pos.getX()).put("y",pos.getY()).put("z",pos.getZ()); return value;
        }
        @Override protected void onStart() {
            var settings=mod.getClientBaritoneSettings(); oldBreak=settings.allowBreak.value; oldPlace=settings.allowPlace.value;
            mod.getBehaviour().push(); mod.getBehaviour().avoidBlockBreaking(p->true);
            settings.allowBreak.value=false; settings.allowPlace.value=false; phaseStarted=System.currentTimeMillis();
        }
        @Override protected Task onTick() {
            if (!isLoaded(pos) || !Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath().equals(name)) {
                failure="not_a_door"; done=true; return null;
            }
            if (!standable(near) || !standable(far)) { failure="door_blocked"; done=true; return null; }
            long elapsed=System.currentTimeMillis()-phaseStarted;
            if (phase==0) {
                if (reached(near)) { next(1); return null; }
                if (elapsed>18000) {
                    if (!flipped) { flipped=true; BlockPos swap=near; near=far; far=swap; side=-side; movement=new GetToBlockTask(near); next(0); }
                    else { failure="unreachable"; done=true; }
                }
                return movement;
            }
            if (phase==1) {
                if (setOpen(true)) { next(2); return null; }
                if (elapsed>2500) { failure="door_wont_open"; done=true; } return null;
            }
            if (phase==2) {
                if (reached(far) && sideNow()!=side) {
                    mod.getInputControls().release(baritone.api.utils.input.Input.MOVE_FORWARD);
                    next(3); return null;
                }
                if (elapsed>5000) { failure="door_blocked"; done=true; return null; }
                mod.getPlayer().setYaw(axis.equals("x") ? (side<0 ? -90 : 90) : (side<0 ? 0 : 180));
                mod.getPlayer().setPitch(0);
                mod.getInputControls().hold(baritone.api.utils.input.Input.MOVE_FORWARD);
                return null;
            }
            if (!close || setOpen(false)) { done=true; return null; }
            if (elapsed>2500) { failure="door_wont_close"; done=true; } return null;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) {
            mod.getClientBaritone().getPathingBehavior().forceCancel();
            mod.getInputControls().release(baritone.api.utils.input.Input.CLICK_RIGHT);
            mod.getInputControls().release(baritone.api.utils.input.Input.MOVE_FORWARD);
            mod.getBehaviour().pop();
            mod.getClientBaritoneSettings().allowBreak.value=oldBreak;
            mod.getClientBaritoneSettings().allowPlace.value=oldPlace;
        }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO use existing door " + pos; }
    }

    private void startSleep(String callId, JsonNode args) {
        BlockPos pos=readPosition(args.path("pos"));
        long duration=args.path("maxDurationMs").asLong(600000);
        if (duration<1000 || duration>900000) throw new IllegalArgumentException("invalid sleep duration");
        if (!isLoaded(pos) || !(mod.getWorld().getBlockState(pos).getBlock() instanceof net.minecraft.block.BedBlock)) {
            result(callId,"failed","no_bed",null); return;
        }
        var state=mod.getWorld().getBlockState(pos);
        if (state.get(net.minecraft.block.BedBlock.PART)==net.minecraft.block.enums.BedPart.HEAD)
            pos=pos.offset(state.get(net.minecraft.block.BedBlock.FACING).getOpposite());
        String refusal=sleepRefusal(pos);
        if (refusal!=null) { result(callId,"failed",refusal,null); return; }
        var task=new IoSleepTask(pos,duration);
        beginTask(callId,task,() -> result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
    }

    private String sleepRefusal(BlockPos pos) {
        if (!mod.getWorld().getDimension().bedWorks()) return "bed_unusable";
        if (!isLoaded(pos) || !(mod.getWorld().getBlockState(pos).getBlock() instanceof net.minecraft.block.BedBlock)) return "bed_unusable";
        var state=mod.getWorld().getBlockState(pos);
        BlockPos head=pos.offset(state.get(net.minecraft.block.BedBlock.FACING));
        if (!isLoaded(head) || !(mod.getWorld().getBlockState(head).getBlock() instanceof net.minecraft.block.BedBlock)) return "bed_unusable";
        long time=Math.floorMod(mod.getWorld().getTimeOfDay(),24000);
        if (!mod.getWorld().isThundering() && !(time>=12542 && time<23459)) return "not_night";
        if (state.get(net.minecraft.block.BedBlock.OCCUPIED) || mod.getWorld().getBlockState(head).get(net.minecraft.block.BedBlock.OCCUPIED)) return "bed_occupied";
        for (var entity:mod.getWorld().getEntities()) if (entity.isAlive() && entity instanceof net.minecraft.entity.mob.HostileEntity
                && Math.abs(entity.getX()-head.getX())<=8 && Math.abs(entity.getY()-head.getY())<=5 && Math.abs(entity.getZ()-head.getZ())<=8) return "monsters_nearby";
        return null;
    }

    private final class IoSleepTask extends Task {
        private final BlockPos pos,head;
        private final long duration;
        private final Task interact;
        private long started,sleptAt,sleptMs,occupiedAt;
        private String failure,wokeBy="morning";
        private boolean done;
        IoSleepTask(BlockPos pos,long duration) { this.pos=pos; this.head=pos.offset(mod.getWorld().getBlockState(pos).get(net.minecraft.block.BedBlock.FACING)); this.duration=duration; interact=new adris.altoclef.tasks.InteractWithBlockTask(pos); }
        private boolean sleepingHere() {
            var sleeping=mod.getPlayer().getSleepingPosition();
            return sleeping.isPresent() && (sleeping.get().equals(pos) || sleeping.get().equals(head));
        }
        private void wake() {
            if (mod.getPlayer().isSleeping() && sleepingHere()) mod.getPlayer().networkHandler.sendPacket(
                new net.minecraft.network.packet.c2s.play.ClientCommandC2SPacket(mod.getPlayer(),net.minecraft.network.packet.c2s.play.ClientCommandC2SPacket.Mode.STOP_SLEEPING));
        }
        ObjectNode value() {
            ObjectNode value=json.createObjectNode().put("slept",sleptAt>0).put("sleptMs",sleptMs).put("wokeBy",wokeBy);
            value.putObject("bed").put("x",pos.getX()).put("y",pos.getY()).put("z",pos.getZ()); return value;
        }
        @Override protected void onStart() { started=System.currentTimeMillis(); StorageHelper.closeScreen(); }
        @Override protected Task onTick() {
            long now=System.currentTimeMillis();
            if (mod.getPlayer().isSleeping()) {
                mod.getInputControls().release(baritone.api.utils.input.Input.CLICK_RIGHT);
                if (!sleepingHere()) { failure="bed_occupied"; done=true; return null; }
                if (sleptAt==0) sleptAt=now;
                sleptMs=now-sleptAt;
                if (sleptMs>=duration) { wokeBy="timeout"; wake(); done=true; }
                return null;
            }
            if (sleptAt>0) { sleptMs=now-sleptAt; done=true; return null; }
            failure=sleepRefusal(pos);
            if ("bed_occupied".equals(failure)) {
                // The bed block update can precede the player's sleeping-position packet.
                if (occupiedAt==0) occupiedAt=now;
                if (now-occupiedAt<500) { failure=null; return null; }
            }
            if (failure!=null) { done=true; return null; }
            if (now-started>35000) { failure="sleep_refused"; done=true; return null; }
            return interact;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) { mod.getInputControls().release(baritone.api.utils.input.Input.CLICK_RIGHT); wake(); }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO sleep in existing bed " + pos; }
    }

    private void startEat(String callId, JsonNode args) {
        if (!"eat".equals(args.path("action").asText())) throw new IllegalArgumentException("unsupported item use");
        int maxFood=args.path("maxFood").asInt(20), untilFood=args.path("untilFood").asInt(-1);
        if (maxFood<0 || maxFood>20 || (args.has("untilFood") && (untilFood<1 || untilFood>20)))
            throw new IllegalArgumentException("invalid hunger threshold");
        var task=new IoEatTask(maxFood,untilFood);
        beginTask(callId,task,() -> result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
    }

    private final class IoEatTask extends Task implements ITaskRequiresGrounded {
        private final int maxFood,untilFood;
        private final java.util.Set<String> unsafe=java.util.Set.of("rotten_flesh","spider_eye","pufferfish","poisonous_potato","suspicious_stew","chicken");
        private final java.util.Set<String> raw=java.util.Set.of("beef","porkchop","mutton","chicken","rabbit","cod","salmon","potato");
        private int before,biteBefore,bites,heldBefore;
        private net.minecraft.item.Item choice;
        private String ate="",failure;
        private boolean previousStop,done;
        private long biteStarted;
        IoEatTask(int maxFood,int untilFood) { this.maxFood=maxFood; this.untilFood=untilFood; }
        private int hunger() { return mod.getPlayer().getHungerManager().getFoodLevel(); }
        private int held(net.minecraft.item.Item item) {
            int count=0;
            for (int i=0;i<36;i++) { ItemStack stack=mod.getPlayer().getInventory().getStack(i); if (stack.isOf(item)) count+=stack.getCount(); }
            return count;
        }
        private net.minecraft.item.Item selectFood() {
            java.util.List<net.minecraft.item.Item> foods=new java.util.ArrayList<>();
            for (int i=0;i<36;i++) {
                ItemStack stack=mod.getPlayer().getInventory().getStack(i);
                var food=adris.altoclef.multiversion.item.ItemVer.getFoodComponent(stack.getItem());
                if (!stack.isEmpty() && food!=null && food.getHunger()>0 && !unsafe.contains(Registries.ITEM.getId(stack.getItem()).getPath())) foods.add(stack.getItem());
            }
            if (foods.stream().anyMatch(item->!raw.contains(Registries.ITEM.getId(item).getPath())))
                foods.removeIf(item->raw.contains(Registries.ITEM.getId(item).getPath()));
            int missing=20-hunger();
            var fits=foods.stream().filter(item->adris.altoclef.multiversion.item.ItemVer.getFoodComponent(item).getHunger()<=missing).toList();
            var comparator=java.util.Comparator.comparingInt((net.minecraft.item.Item item)->adris.altoclef.multiversion.item.ItemVer.getFoodComponent(item).getHunger());
            return fits.isEmpty() ? foods.stream().min(comparator).orElse(null) : fits.stream().max(comparator).orElse(null);
        }
        ObjectNode value() { return json.createObjectNode().put("ate",ate).put("foodBefore",before).put("foodAfter",hunger()); }
        @Override protected void onStart() {
            before=hunger(); previousStop=mod.getFoodChain().isShouldStop(); mod.getFoodChain().shouldStop(true);
            StorageHelper.closeScreen();
        }
        @Override protected Task onTick() {
            if (before>maxFood && bites==0) { failure="already_full"; done=true; return null; }
            if (choice!=null && held(choice)<heldBefore && hunger()>biteBefore) {
                ate=Registries.ITEM.getId(choice).getPath(); bites++; choice=null;
                mod.getInputControls().release(baritone.api.utils.input.Input.CLICK_RIGHT);
                mod.getExtraBaritoneSettings().setInteractionPaused(false);
                if (untilFood<0 || hunger()>=untilFood || bites>=8) { done=true; return null; }
            }
            if (choice==null) {
                choice=selectFood();
                if (choice==null) { if (bites==0) failure="no_food"; done=true; return null; }
                if (hunger()>=20) { failure="did_not_eat"; done=true; return null; }
                biteBefore=hunger(); heldBefore=held(choice); biteStarted=System.currentTimeMillis();
            }
            if (System.currentTimeMillis()-biteStarted>6000) { failure="did_not_eat"; done=true; return null; }
            if (adris.altoclef.util.helpers.LookHelper.tryAvoidingInteractable(mod) && mod.getSlotHandler().forceEquipItem(new net.minecraft.item.Item[]{choice},true)) {
                mod.getInputControls().hold(baritone.api.utils.input.Input.CLICK_RIGHT);
                mod.getExtraBaritoneSettings().setInteractionPaused(true);
            }
            return null;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) {
            mod.getInputControls().release(baritone.api.utils.input.Input.CLICK_RIGHT);
            mod.getExtraBaritoneSettings().setInteractionPaused(false);
            mod.getFoodChain().shouldStop(previousStop);
        }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO eat held safe food"; }
    }

    private void startPickup(String callId, JsonNode args) {
        int radius=args.path("radius").asInt(8);
        if (radius<1 || radius>32) throw new IllegalArgumentException("invalid pickup radius");
        java.util.Set<String> names=new java.util.HashSet<>();
        if (args.has("names")) {
            if (!args.path("names").isArray() || args.path("names").isEmpty() || args.path("names").size()>16)
                throw new IllegalArgumentException("invalid pickup names");
            for (var name:args.path("names")) names.add(name.asText());
        }
        net.minecraft.util.math.Vec3d near=null;
        if (args.has("near")) {
            var p=args.path("near");
            if (!p.path("x").isNumber() || !p.path("y").isNumber() || !p.path("z").isNumber()) throw new IllegalArgumentException("invalid pickup centre");
            near=new net.minecraft.util.math.Vec3d(p.path("x").asDouble(),p.path("y").asDouble(),p.path("z").asDouble());
            if (!Double.isFinite(near.x) || !Double.isFinite(near.y) || !Double.isFinite(near.z) || Math.abs(near.x)>30000000 || Math.abs(near.z)>30000000 || near.y< -64 || near.y>320) throw new IllegalArgumentException("invalid pickup centre");
        }
        boolean approach=args.path("approach").asBoolean(false);
        if (approach && near==null) throw new IllegalArgumentException("approach needs a pickup centre");
        var task=new IoPickupTask(radius,names,near,approach);
        beginTask(callId,task,() -> result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
    }

    private final class IoPickupTask extends Task implements ITaskRequiresGrounded {
        private final int radius;
        private final java.util.Set<String> names;
        private final java.util.Map<String,Integer> before=new java.util.HashMap<>();
        private final java.util.List<net.minecraft.entity.ItemEntity> seen=new java.util.ArrayList<>();
        private final java.util.Map<Integer,Integer> amounts=new java.util.HashMap<>();
        private final java.util.Map<Integer,String> kinds=new java.util.HashMap<>();
        private final java.util.Set<Integer> skipped=new java.util.HashSet<>();
        private net.minecraft.util.math.Vec3d centre;
        private net.minecraft.entity.ItemEntity active;
        private Task movement;
        private long started,targetStarted,approachStarted;
        private boolean done,approaching,oldBreak,oldPlace;
        private final net.minecraft.util.math.Vec3d requestedCentre;
        private final Task approachTask;
        private String failure;
        IoPickupTask(int radius,java.util.Set<String> names,net.minecraft.util.math.Vec3d near,boolean approach) {
            this.radius=radius; this.names=names; requestedCentre=near; approaching=approach;
            approachTask=approach ? new GetToBlockTask(BlockPos.ofFloored(near),false) : null;
        }
        private java.util.Map<String,Integer> inventory() {
            java.util.Map<String,Integer> counts=new java.util.HashMap<>();
            for (int i=0;i<36;i++) {
                ItemStack stack=mod.getPlayer().getInventory().getStack(i);
                if (!stack.isEmpty()) counts.merge(Registries.ITEM.getId(stack.getItem()).getPath(),stack.getCount(),Integer::sum);
            }
            return counts;
        }
        private boolean room(ItemStack item) {
            for (int i=0;i<36;i++) {
                ItemStack held=mod.getPlayer().getInventory().getStack(i);
                if (held.isEmpty() || (ItemStack.areItemsAndComponentsEqual(held,item) && held.getCount()<held.getMaxCount())) return true;
            }
            return false;
        }
        private int collected() {
            var budget=inventory();
            budget.replaceAll((name,count)->Math.max(0,count-before.getOrDefault(name,0)));
            int total=0;
            for (var entity:seen) {
                String name=kinds.get(entity.getId()); int available=budget.getOrDefault(name,0);
                int removed=entity.isAlive() ? Math.max(0,amounts.get(entity.getId())-entity.getStack().getCount()) : amounts.get(entity.getId());
                int gained=Math.min(available,removed);
                if (gained>0) { total++; budget.put(name,available-gained); }
            }
            return total;
        }
        ObjectNode value() {
            ObjectNode value=json.createObjectNode().put("seen",seen.size()).put("collected",collected());
            if (centre!=null) value.putObject("centre").put("x",centre.x).put("y",centre.y).put("z",centre.z);
            ObjectNode counts=value.putObject("inventoryBefore"); before.forEach(counts::put);
            return value;
        }
        private void scanDrops() {
            for (var entity:mod.getEntityTracker().getDroppedItems()) {
                String name=Registries.ITEM.getId(entity.getStack().getItem()).getPath();
                if (entity.isAlive() && !kinds.containsKey(entity.getId()) && entity.squaredDistanceTo(centre)<=radius*radius && (names.isEmpty() || names.contains(name))) {
                    seen.add(entity); amounts.put(entity.getId(),entity.getStack().getCount()); kinds.put(entity.getId(),name);
                    mod.getBehaviour().addProtectedItems(entity.getStack().getItem());
                }
            }
        }
        @Override protected void onStart() {
            centre=requestedCentre==null ? mod.getPlayer().getPos() : requestedCentre;
            started=System.currentTimeMillis(); approachStarted=started; before.putAll(inventory());
            mod.getBehaviour().push();
            if (approachTask!=null) {
                var settings=mod.getClientBaritoneSettings(); oldBreak=settings.allowBreak.value; oldPlace=settings.allowPlace.value;
                settings.allowBreak.value=false; settings.allowPlace.value=false; mod.getBehaviour().avoidBlockBreaking(p->true);
            }
            scanDrops();
        }
        @Override protected Task onTick() {
            long now=System.currentTimeMillis();
            if (approaching) {
                // Observe drops while approaching so pickups during the walk still count.
                scanDrops();
                if (mod.getPlayer().squaredDistanceTo(centre)>9) {
                    if (now-approachStarted>180000) { failure="death_site_unreachable"; done=true; return null; }
                    return approachTask;
                }
                approaching=false; started=now;
            }
            if (active!=null && (!active.isAlive() || active.squaredDistanceTo(centre)>radius*radius || now-targetStarted>8000)) {
                skipped.add(active.getId()); active=null; movement=null;
            }
            if (active==null) {
                active=seen.stream().filter(e->e.isAlive() && !skipped.contains(e.getId()) && e.squaredDistanceTo(centre)<=radius*radius && room(e.getStack()))
                    .min(java.util.Comparator.comparingDouble(e->e.squaredDistanceTo(mod.getPlayer()))).orElse(null);
                if (active!=null) { targetStarted=now; movement=new GetToEntityTask(active,0.25); }
            }
            if (active==null || now-started>100000) {
                if (!seen.isEmpty() && collected()==0)
                    failure=seen.stream().anyMatch(e->e.isAlive()) && seen.stream().filter(e->e.isAlive()).noneMatch(e->room(e.getStack())) ? "inventory_full" : "pickup_failed";
                done=true; return null;
            }
            return movement;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) {
            mod.getBehaviour().pop(); mod.getClientBaritone().getPathingBehavior().forceCancel();
            if (approachTask!=null) {
                mod.getClientBaritoneSettings().allowBreak.value=oldBreak; mod.getClientBaritoneSettings().allowPlace.value=oldPlace;
            }
        }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO pick up existing drops"; }
    }

    private void startContainerTransfer(String callId, JsonNode args, boolean deposit) {
        BlockPos pos = readPosition(args.path("pos"));
        String item = args.path("item").asText();
        var id = net.minecraft.util.Identifier.of("minecraft", item);
        if (!Registries.ITEM.containsId(id)) { result(callId,"failed","item_not_present",null); return; }
        int count = args.has("count") ? args.path("count").asInt() : 2304;
        if (count < 1 || count > 2304) throw new IllegalArgumentException("invalid transfer count");
        if (!isLoaded(pos)) { result(callId,"failed","container_not_found",null); return; }
        String name = Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath();
        if (!storageContainer(name)) { result(callId,"failed","not_a_container",null); return; }
        var task = new IoContainerTransferTask(pos,name,Registries.ITEM.get(id),item,count,deposit);
        beginTask(callId,task,() -> result(callId,task.failure == null ? "ok" : "failed",task.failure,task.value()),task::value);
    }

    /** Move held stacks without gathering, discarding, or leaving a partial stack on the cursor. */
    private final class IoContainerTransferTask extends Task implements ITaskRequiresGrounded {
        private final BlockPos pos;
        private final String name, itemName;
        private final net.minecraft.item.Item item;
        private final int requested;
        private final boolean deposit;
        private final Task interact;
        private net.minecraft.screen.ScreenHandler handler;
        private int storageSlots, beforeSource, beforeDestination, wanted, moved, sourceSlot=-1;
        private long lastClick;
        private boolean done;
        private String failure;
        private ObjectNode observed;
        IoContainerTransferTask(BlockPos pos, String name, net.minecraft.item.Item item, String itemName, int requested, boolean deposit) {
            this.pos=pos; this.name=name; this.item=item; this.itemName=itemName;
            this.requested=requested; this.deposit=deposit;
            interact=new adris.altoclef.tasks.InteractWithBlockTask(pos);
        }
        private int count(boolean source) {
            int total=0;
            boolean storage = source != deposit;
            int start=storage ? 0 : storageSlots, end=storage ? storageSlots : handler.slots.size();
            for (int i=start;i<end;i++) {
                ItemStack stack=handler.getSlot(i).getStack();
                if (stack.isOf(item)) total+=stack.getCount();
            }
            return total;
        }
        private int destination(ItemStack cursor, boolean source) {
            boolean storage=source != deposit;
            int start=storage ? 0 : storageSlots, end=storage ? storageSlots : handler.slots.size();
            // Fill compatible stacks first; never swap a stack with different components.
            for (int pass=0;pass<2;pass++) for (int i=start;i<end;i++) {
                var slot=handler.getSlot(i); ItemStack stack=slot.getStack();
                if (!slot.canInsert(cursor)) continue;
                if (stack.isEmpty()) { if (pass==1) return i; }
                else if (pass==0 && ItemStack.areItemsAndComponentsEqual(stack,cursor)
                        && stack.getCount()<slot.getMaxItemCount(cursor)) return i;
            }
            return -1;
        }
        private void click(int index, int button) {
            mod.getController().clickSlot(handler.syncId,index,button,net.minecraft.screen.slot.SlotActionType.PICKUP,mod.getPlayer());
            mod.getSlotHandler().registerSlotAction();
            lastClick=System.currentTimeMillis();
        }
        private boolean restoreCursor(boolean force) {
            ItemStack cursor=handler.getCursorStack();
            if (cursor.isEmpty()) return true;
            int target=-1;
            if (sourceSlot>=0) {
                var slot=handler.getSlot(sourceSlot); var stack=slot.getStack();
                if (slot.canInsert(cursor) && (stack.isEmpty() || (ItemStack.areItemsAndComponentsEqual(stack,cursor)
                        && stack.getCount()+cursor.getCount()<=slot.getMaxItemCount(cursor)))) target=sourceSlot;
            }
            if (target<0) target=destination(cursor,true);
            if (target<0) { failure="container_full"; return false; }
            if (force) {
                mod.getController().clickSlot(handler.syncId,target,0,net.minecraft.screen.slot.SlotActionType.PICKUP,mod.getPlayer());
                mod.getSlotHandler().registerSlotAction();
            } else click(target,0);
            return handler.getCursorStack().isEmpty();
        }
        ObjectNode value() {
            ObjectNode value=json.createObjectNode().put("item",itemName).put("moved",moved);
            if (handler != null) {
                value.put("inventoryDelta",deposit ? beforeSource-count(true) : count(false)-beforeDestination);
                value.put("containerDelta",deposit ? count(false)-beforeDestination : beforeSource-count(true));
            }
            if (observed != null) value.set("observation",observed.deepCopy());
            return value;
        }
        @Override protected void onStart() {
            StorageHelper.closeScreen(); syncedInventoryHandler=null;
            mod.getBehaviour().push(); mod.getBehaviour().addProtectedItems(item);
        }
        @Override protected Task onTick() {
            if (!isLoaded(pos)) { failure="container_not_found"; done=true; return null; }
            if (!Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath().equals(name)) {
                failure="not_a_container"; done=true; return null;
            }
            if (handler==null) {
                if (containerSnapshot(pos)==null) return interact;
                handler=mod.getPlayer().currentScreenHandler; storageSlots=handler.slots.size()-36;
                beforeSource=count(true); beforeDestination=count(false); wanted=Math.min(requested,beforeSource);
            }
            if (mod.getPlayer().currentScreenHandler!=handler) { failure="open_failed"; done=true; return null; }
            moved=count(false)-beforeDestination;
            if (!mod.getSlotHandler().canDoSlotAction()) return null;
            if (moved>=wanted || failure!=null) {
                if (!restoreCursor(false)) return null;
                if (System.currentTimeMillis()-lastClick<400) return null;
                observed=containerSnapshot(pos);
                if (beforeSource-count(true)!=moved || moved<0 || moved>wanted) failure="transfer_mismatch";
                done=true; return null;
            }
            ItemStack cursor=handler.getCursorStack();
            if (cursor.isEmpty()) {
                boolean storage=!deposit;
                int start=storage ? 0 : storageSlots, end=storage ? storageSlots : handler.slots.size();
                for (int i=start;i<end;i++) {
                    ItemStack stack=handler.getSlot(i).getStack();
                    if (stack.isOf(item) && destination(stack,false)>=0) { sourceSlot=i; click(i,0); return null; }
                }
                failure="container_full"; return null;
            }
            int target=destination(cursor,false);
            if (target<0) { failure="container_full"; return null; }
            click(target,cursor.getCount()<=wanted-moved ? 0 : 1);
            return null;
        }
        @Override public boolean isFinished() { return done || (failure!=null && handler==null); }
        @Override protected void onStop(Task interrupt) {
            if (handler!=null && mod.getPlayer().currentScreenHandler==handler) {
                restoreCursor(true);
                moved=count(false)-beforeDestination;
                observed=containerSnapshot(pos);
                // Closing a withdrawal with a nonempty cursor would give unrequested stock to the player.
                if (handler.getCursorStack().isEmpty()) StorageHelper.closeScreen();
            } else StorageHelper.closeScreen();
            mod.getBehaviour().pop();
        }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO " + (deposit ? "deposit " : "withdraw ") + itemName; }
    }

    private void startObserveContainer(String callId, JsonNode args) {
        boolean furnace="observe_furnace".equals(args.path("interaction").asText());
        if (!furnace && !"observe_container".equals(args.path("interaction").asText()))
            throw new IllegalArgumentException("unsupported activation");
        BlockPos pos = readPosition(args.path("pos"));
        if (!isLoaded(pos)) { result(callId,"failed","container_not_found",null); return; }
        String name = Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath();
        if (furnace ? !furnace(name) : !storageContainer(name)) { result(callId,"failed",furnace ? "no_furnace" : "not_a_container",null); return; }
        var task = new IoObserveContainerTask(pos, name, furnace);
        beginTask(callId,task,() -> result(callId,task.failure == null ? "ok" : "failed",task.failure,task.observed),
                () -> task.observed == null ? json.createObjectNode() : task.observed.deepCopy());
    }

    private final class IoObserveContainerTask extends Task implements ITaskRequiresGrounded {
        private final BlockPos pos;
        private final String name;
        private final boolean furnace;
        private final Task interact;
        private String failure;
        private ObjectNode observed;
        IoObserveContainerTask(BlockPos pos, String name, boolean furnace) {
            this.pos=pos; this.name=name; this.furnace=furnace;
            interact=new adris.altoclef.tasks.InteractWithBlockTask(pos);
        }
        @Override protected void onStart() { StorageHelper.closeScreen(); syncedInventoryHandler=null; }
        @Override protected Task onTick() {
            if (!isLoaded(pos)) { failure="container_not_found"; return null; }
            if (!Registries.BLOCK.getId(mod.getWorld().getBlockState(pos).getBlock()).getPath().equals(name)) {
                failure="not_a_container"; return null;
            }
            observed=screenSnapshot(pos,furnace);
            if (observed != null) return null;
            return interact;
        }
        @Override public boolean isFinished() { return failure != null || observed != null; }
        @Override protected void onStop(Task interrupt) { StorageHelper.closeScreen(); }
        @Override protected boolean isEqual(Task other) { return this == other; }
        @Override protected String toDebugString() { return "IO observe container " + pos; }
    }

    private void startPlace(String callId, JsonNode args) {
        BlockPos pos = readPosition(args.path("pos"));
        String name = args.path("block").asText();
        var id = net.minecraft.util.Identifier.of("minecraft", name);
        if (!Registries.BLOCK.containsId(id) || !(Registries.ITEM.get(id) instanceof net.minecraft.item.BlockItem)) {
            result(callId, "failed", "unknown_block", null); return;
        }
        if (args.path("stationary").asBoolean(false)) {
            var task=new IoStationaryPlaceTask(pos,Registries.BLOCK.get(id),name);
            beginTask(callId,task,()->result(callId,task.failure==null ? "ok" : "failed",task.failure,task.value()),task::value);
            return;
        }
        var task = new IoPlaceTask(pos, Registries.BLOCK.get(id), name);
        PartialSupplier observed = () -> {
            ObjectNode value = json.createObjectNode().put("block", name);
            value.putObject("pos").put("x", pos.getX()).put("y", pos.getY()).put("z", pos.getZ());
            return value;
        };
        beginTask(callId, task, () -> result(callId, task.failure == null ? "ok" : "failed", task.failure, observed.get()), observed);
    }

    /** Place a nearby supported block without asking the builder to climb out of an enclosure. */
    private final class IoStationaryPlaceTask extends Task {
        private final BlockPos pos;
        private final net.minecraft.block.Block block;
        private final String name;
        private BlockPos feet;
        private long started,clicked;
        private int attempts;
        private boolean done;
        private String failure;
        IoStationaryPlaceTask(BlockPos pos,net.minecraft.block.Block block,String name) { this.pos=pos; this.block=block; this.name=name; }
        ObjectNode value() {
            var value=json.createObjectNode().put("block",name);
            value.putObject("pos").put("x",pos.getX()).put("y",pos.getY()).put("z",pos.getZ()); return value;
        }
        @Override protected void onStart() {
            feet=mod.getPlayer().getBlockPos(); started=System.currentTimeMillis();
            mod.getClientBaritone().getPathingBehavior().forceCancel(); mod.getBehaviour().push();
            mod.getBehaviour().addProtectedItems(block.asItem());
        }
        @Override protected Task onTick() {
            if (!feet.equals(mod.getPlayer().getBlockPos())) { failure="place_failed"; done=true; return null; }
            if (!isLoaded(pos)) { failure="place_failed"; done=true; return null; }
            var state=mod.getWorld().getBlockState(pos);
            if (state.isOf(block)) { done=true; return null; }
            if (!state.isReplaceable() || !state.getFluidState().isEmpty()) { failure="position_occupied"; done=true; return null; }
            if (mod.getPlayer().getBoundingBox().intersects(new net.minecraft.util.math.Box(pos))) { failure="position_occupied"; done=true; return null; }
            if (countOf(name)<1) { failure="not_in_inventory"; done=true; return null; }
            long now=System.currentTimeMillis();
            if (now-started>3000) { failure="place_failed"; done=true; return null; }
            if (clicked!=0 && now-clicked<600) return null;
            if (attempts>=3) { failure="place_failed"; done=true; return null; }
            for (var direction:net.minecraft.util.math.Direction.values()) {
                var support=pos.offset(direction);
                if (!isLoaded(support) || !mod.getWorld().getBlockState(support).isSolidBlock(mod.getWorld(),support)) continue;
                var face=direction.getOpposite();
                var hit=net.minecraft.util.math.Vec3d.ofCenter(support).add(face.getOffsetX()*0.5,face.getOffsetY()*0.5,face.getOffsetZ()*0.5);
                if (mod.getPlayer().getEyePos().squaredDistanceTo(hit)>20.25) continue;
                if (!mod.getSlotHandler().forceEquipItem(new net.minecraft.item.Item[]{block.asItem()},true)) return null;
                adris.altoclef.util.helpers.LookHelper.lookAt(mod,hit,false);
                mod.getController().interactBlock(mod.getPlayer(),net.minecraft.util.Hand.MAIN_HAND,new net.minecraft.util.hit.BlockHitResult(hit,face,support,false));
                clicked=now; attempts++; return null;
            }
            failure="nothing_to_place_against"; done=true; return null;
        }
        @Override public boolean isFinished() { return done; }
        @Override protected void onStop(Task interrupt) { mod.getBehaviour().pop(); mod.getClientBaritone().getPathingBehavior().forceCancel(); }
        @Override protected boolean isEqual(Task other) { return this==other; }
        @Override protected String toDebugString() { return "IO stationary place " + pos; }
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
