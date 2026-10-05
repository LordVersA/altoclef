package adris.altoclef.io;

import com.fasterxml.jackson.databind.JsonNode;
import net.minecraft.block.Block;
import net.minecraft.entity.Entity;
import net.minecraft.entity.LivingEntity;
import net.minecraft.entity.mob.HostileEntity;
import net.minecraft.entity.mob.MobEntity;
import net.minecraft.entity.mob.PhantomEntity;
import net.minecraft.entity.mob.SlimeEntity;
import net.minecraft.entity.passive.TameableEntity;
import net.minecraft.entity.player.PlayerEntity;
import net.minecraft.item.ItemStack;
import net.minecraft.registry.Registries;
import net.minecraft.util.math.BlockPos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Core's policy as the body enforces it (plan 6.7): a read-only copy of the `core.policy` document
 * and the questions the action-boundary hooks ask of it. The mod never decides what is allowed; it
 * only answers from what Core said.
 */
public final class IoPolicy {
    public record Region(String name, String dimension, int minX, int minY, int minZ, int maxX, int maxY, int maxZ) {
        boolean contains(BlockPos pos, String dim) {
            if (dimension != null && !dimension.equals(dim)) return false;
            return pos.getX() >= minX && pos.getX() <= maxX && pos.getY() >= minY && pos.getY() <= maxY
                    && pos.getZ() >= minZ && pos.getZ() <= maxZ;
        }
    }

    private static volatile IoPolicy current;
    public static IoPolicy current() { return current; }
    public static void install(IoPolicy policy) { current = policy; }
    public static void clear() { current = null; }

    public final int version;
    private final List<Region> regions = new ArrayList<>();
    private final boolean playerMadeRule;
    private final Set<String> allowedDimensions;
    private final Set<String> protectedItems = new HashSet<>();
    private final boolean namedItems, enchantedItems;
    private final boolean ruleNamed, ruleTamed, ruleLeashed, rulePlayers;
    private final Set<String> exemptions = new HashSet<>();

    public IoPolicy(JsonNode doc) {
        version = doc.path("version").asInt();
        for (JsonNode r : doc.path("protectedRegions")) {
            JsonNode a = r.path("min"), b = r.path("max");
            regions.add(new Region(r.path("name").asText(), r.path("dimension").isNull() ? null : r.path("dimension").asText(),
                    (int) Math.floor(Math.min(a.path("x").asDouble(), b.path("x").asDouble())),
                    (int) Math.floor(Math.min(a.path("y").asDouble(), b.path("y").asDouble())),
                    (int) Math.floor(Math.min(a.path("z").asDouble(), b.path("z").asDouble())),
                    (int) Math.floor(Math.max(a.path("x").asDouble(), b.path("x").asDouble())),
                    (int) Math.floor(Math.max(a.path("y").asDouble(), b.path("y").asDouble())),
                    (int) Math.floor(Math.max(a.path("z").asDouble(), b.path("z").asDouble()))));
        }
        playerMadeRule = doc.path("playerMadeRule").asBoolean();
        if (doc.path("allowedDimensions").isArray()) {
            allowedDimensions = new HashSet<>();
            for (JsonNode d : doc.path("allowedDimensions")) allowedDimensions.add(d.asText());
        } else allowedDimensions = null;
        for (JsonNode n : doc.path("protectedItems").path("names")) protectedItems.add(n.asText());
        namedItems = doc.path("protectedItems").path("named").asBoolean();
        enchantedItems = doc.path("protectedItems").path("enchanted").asBoolean();
        JsonNode e = doc.path("entityRules");
        rulePlayers = e.path("players").asBoolean(); ruleNamed = e.path("named").asBoolean();
        ruleTamed = e.path("tamed").asBoolean(); ruleLeashed = e.path("leashed").asBoolean();
        for (JsonNode x : doc.path("exemptions")) exemptions.add(x.asText());
    }

    public List<Region> regions() { return regions; }
    public boolean exempt(String what) { return exemptions.contains(what); }

    /** The name of the protected region holding the position, or null. */
    public String regionAt(BlockPos pos, String dimension) {
        for (Region r : regions) if (r.contains(pos, dimension)) return r.name();
        return null;
    }

    public boolean dimensionAllowed(String dimension) {
        return allowedDimensions == null || allowedDimensions.contains(dimension);
    }

    /** Why breaking this block is refused, or null. */
    public String breakRefusal(BlockPos pos, Block block, String dimension) {
        String region = regionAt(pos, dimension);
        if (region != null) return "protected_region:" + region;
        if (playerMadeRule && isPlayerMade(Registries.BLOCK.getId(block).getPath())) return "player_made";
        return null;
    }

    /** Why this item must not be thrown away, or null. */
    public String dropRefusal(ItemStack stack) {
        if (stack.isEmpty()) return null;
        if (protectedItems.contains(Registries.ITEM.getId(stack.getItem()).getPath())) return "protected_item";
        if (namedItems && stack.contains(net.minecraft.component.DataComponentTypes.CUSTOM_NAME)) return "named_item";
        if (enchantedItems && (stack.hasEnchantments())) return "enchanted_item";
        return null;
    }

    /** Why this entity may not be attacked, or null. Hostile mobs are exempt when the policy says so. */
    public String attackRefusal(Entity entity) {
        boolean hostile = entity instanceof HostileEntity || entity instanceof SlimeEntity || entity instanceof PhantomEntity;
        if (rulePlayers && entity instanceof PlayerEntity) return "player";
        if (hostile && exempt("attack_hostile") && !(entity instanceof TameableEntity t && t.isTamed())) return null;
        if (entity instanceof LivingEntity) {
            if (ruleNamed && entity.hasCustomName()) return "named_entity";
            if (ruleTamed && entity instanceof TameableEntity tame && tame.isTamed()) return "tamed_entity";
            if (ruleLeashed && entity instanceof MobEntity mob && mob.isLeashed()) return "leashed_entity";
        }
        return null;
    }

    // ---- the player-made rule: the same table as packages/shared/src/player-made.ts, checked by shared cases ----

    private static final String[] SUFFIXES = {
        "_planks", "_slab", "_stairs", "_fence", "_fence_gate", "_door", "_trapdoor", "_wall", "_sign", "_hanging_sign",
        "_bed", "_banner", "_bricks", "_brick", "_wool", "_carpet", "_concrete", "_concrete_powder", "_glazed_terracotta",
        "_stained_glass", "_stained_glass_pane", "_button", "_pressure_plate", "_shulker_box", "_lantern", "_candle"
    };
    private static final Set<String> EXACT = Set.of(
        "glass", "glass_pane", "bricks", "chest", "trapped_chest", "ender_chest", "barrel", "crafting_table", "furnace",
        "blast_furnace", "smoker", "smithing_table", "cartography_table", "fletching_table", "loom", "grindstone",
        "stonecutter", "enchanting_table", "anvil", "chipped_anvil", "damaged_anvil", "bookshelf", "lectern", "jukebox",
        "note_block", "torch", "soul_torch", "lantern", "soul_lantern", "ladder", "rail", "powered_rail", "detector_rail",
        "activator_rail", "lever", "redstone_lamp", "hopper", "dispenser", "dropper", "piston", "sticky_piston", "observer",
        "repeater", "comparator", "tnt", "bell", "campfire", "soul_campfire", "composter", "cauldron", "flower_pot",
        "item_frame", "glow_item_frame", "painting", "beacon", "conduit", "smooth_stone", "cut_sandstone",
        "cut_red_sandstone", "polished_andesite", "polished_diorite", "polished_granite", "stone_bricks",
        "mossy_stone_bricks", "cracked_stone_bricks", "chiseled_stone_bricks");

    public static boolean isPlayerMade(String block) {
        String name = block.replaceFirst("^minecraft:", "").toLowerCase();
        if (EXACT.contains(name)) return true;
        for (String suffix : SUFFIXES) if (name.endsWith(suffix)) return true;
        return false;
    }
}
