package adris.altoclef.mixins;

import adris.altoclef.AltoClef;
import adris.altoclef.io.IoBridge;
import adris.altoclef.io.IoPolicy;
import net.minecraft.block.BlockState;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.network.ClientPlayerEntity;
import net.minecraft.client.network.ClientPlayerInteractionManager;
import net.minecraft.entity.Entity;
import net.minecraft.entity.player.PlayerEntity;
import net.minecraft.item.BlockItem;
import net.minecraft.item.BucketItem;
import net.minecraft.item.ItemStack;
import net.minecraft.item.Items;
import net.minecraft.screen.slot.SlotActionType;
import net.minecraft.util.ActionResult;
import net.minecraft.util.Hand;
import net.minecraft.util.hit.BlockHitResult;
import net.minecraft.util.hit.HitResult;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.math.Direction;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * The client's interaction layer, where every action passes whoever asked for it: an AltoClef task,
 * Baritone, a survival chain or a primitive from a program. Each hook asks Core's policy and, when it
 * says no, refuses the action as if the server had, and reports a `policy_blocked` event (plan 6.7).
 */
@Mixin(ClientPlayerInteractionManager.class)
public final class IoPolicyMixin {
    private static String dimension() {
        var world = MinecraftClient.getInstance().world;
        return world == null ? "" : world.getRegistryKey().getValue().toString();
    }

    private static String breakRefusal(BlockPos pos) {
        IoPolicy policy = IoPolicy.current();
        var world = MinecraftClient.getInstance().world;
        if (policy == null || world == null) return null;
        BlockState state = world.getBlockState(pos);
        String refusal = policy.breakRefusal(pos, state.getBlock(), dimension());
        if (refusal != null) IoBridge.policyBlocked("break", refusal, pos.getX() + "," + pos.getY() + "," + pos.getZ());
        return refusal;
    }

    @Inject(method = "attackBlock", at = @At("HEAD"), cancellable = true)
    private void io$attackBlock(BlockPos pos, Direction direction, CallbackInfoReturnable<Boolean> cir) {
        if (breakRefusal(pos) != null) cir.setReturnValue(false);
    }

    @Inject(method = "updateBlockBreakingProgress", at = @At("HEAD"), cancellable = true)
    private void io$continueBreaking(BlockPos pos, Direction direction, CallbackInfoReturnable<Boolean> cir) {
        if (breakRefusal(pos) != null) cir.setReturnValue(false);
    }

    @Inject(method = "breakBlock", at = @At("HEAD"), cancellable = true)
    private void io$breakBlock(BlockPos pos, CallbackInfoReturnable<Boolean> cir) {
        if (breakRefusal(pos) != null) cir.setReturnValue(false);
    }

    /** Whether the stack in hand changes the world where it is used: a block, a bucket, fire, a tool that tills or strips. */
    private static boolean altersWorld(ItemStack stack) {
        var item = stack.getItem();
        return item instanceof BlockItem || item instanceof BucketItem || item == Items.FLINT_AND_STEEL
                || item == Items.FIRE_CHARGE || item == Items.BONE_MEAL || item instanceof net.minecraft.item.HoeItem
                || item instanceof net.minecraft.item.AxeItem || item instanceof net.minecraft.item.ShovelItem;
    }

    /** Placing or picking up water to survive a fall is the one thing that skips the regions (6.7). */
    private static boolean fallWater(ItemStack stack) {
        IoPolicy policy = IoPolicy.current();
        if (policy == null || !policy.exempt("fall_water")) return false;
        var item = stack.getItem();
        if (item != Items.WATER_BUCKET && item != Items.BUCKET) return false;
        var mlg = AltoClef.getInstance().getMLGBucketChain();
        return !mlg.doneMLG() || mlg.isFalling(AltoClef.getInstance());
    }

    private static String placeRefusal(ItemStack stack, BlockPos pos) {
        IoPolicy policy = IoPolicy.current();
        if (policy == null || stack.isEmpty() || !altersWorld(stack) || fallWater(stack)) return null;
        String region = policy.regionAt(pos, dimension());
        if (region == null) return null;
        IoBridge.policyBlocked("place", "protected_region:" + region, pos.getX() + "," + pos.getY() + "," + pos.getZ());
        return region;
    }

    @Inject(method = "interactBlock", at = @At("HEAD"), cancellable = true)
    private void io$interactBlock(ClientPlayerEntity player, Hand hand, BlockHitResult hit, CallbackInfoReturnable<ActionResult> cir) {
        ItemStack held = player.getStackInHand(hand);
        BlockState state = player.getWorld().getBlockState(hit.getBlockPos());
        // A normal right-click opens these screens before the held item can change the world.
        // In particular, an axe in hand must not turn opening a named chest into a refused
        // placement. Sneaking still permits item use against the block and stays guarded.
        if (!player.isSneaking() && (state.getBlock() instanceof net.minecraft.block.ChestBlock
                || state.getBlock() instanceof net.minecraft.block.BarrelBlock
                || state.getBlock() instanceof net.minecraft.block.ShulkerBoxBlock
                || state.getBlock() instanceof net.minecraft.block.AbstractFurnaceBlock)) return;
        // A block goes beside the one clicked; anything else acts on the one clicked.
        BlockPos at = held.getItem() instanceof BlockItem ? hit.getBlockPos().offset(hit.getSide()) : hit.getBlockPos();
        if (placeRefusal(held, at) != null || (held.getItem() instanceof BlockItem && placeRefusal(held, hit.getBlockPos()) != null))
            cir.setReturnValue(ActionResult.FAIL);
    }

    /** Buckets pour and scoop where the player looks, without clicking a block. */
    @Inject(method = "interactItem", at = @At("HEAD"), cancellable = true)
    private void io$interactItem(PlayerEntity player, Hand hand, CallbackInfoReturnable<ActionResult> cir) {
        ItemStack held = player.getStackInHand(hand);
        if (!(held.getItem() instanceof BucketItem)) return;
        HitResult target = player.raycast(player.getBlockInteractionRange(), 1.0f, true);
        if (!(target instanceof BlockHitResult block)) return;
        if (placeRefusal(held, block.getBlockPos().offset(block.getSide())) != null || placeRefusal(held, block.getBlockPos()) != null)
            cir.setReturnValue(ActionResult.FAIL);
    }

    private static boolean attackRefused(Entity entity) {
        IoPolicy policy = IoPolicy.current();
        if (policy == null) return false;
        String refusal = policy.attackRefusal(entity);
        if (refusal != null) IoBridge.policyBlocked("attack", refusal, entity.getType().getUntranslatedName() + "#" + entity.getId());
        return refusal != null;
    }

    @Inject(method = "attackEntity", at = @At("HEAD"), cancellable = true)
    private void io$attackEntity(PlayerEntity player, Entity target, CallbackInfo ci) {
        if (attackRefused(target)) ci.cancel();
    }

    private static boolean dropRefused(ItemStack stack) {
        IoPolicy policy = IoPolicy.current();
        if (policy == null || IoBridge.dropAuthorizedNow()) return false;
        String refusal = policy.dropRefusal(stack);
        if (refusal != null)
            IoBridge.policyBlocked("drop", refusal, net.minecraft.registry.Registries.ITEM.getId(stack.getItem()).getPath());
        return refusal != null;
    }

    /** Throwing a slot, or letting go of the cursor outside the window, drops what is in it. */
    @Inject(method = "clickSlot", at = @At("HEAD"), cancellable = true)
    private void io$clickSlot(int syncId, int slotId, int button, SlotActionType action, PlayerEntity player, CallbackInfo ci) {
        var handler = player.currentScreenHandler;
        if (handler == null || handler.syncId != syncId) return;
        ItemStack dropped = ItemStack.EMPTY;
        if (action == SlotActionType.THROW && slotId >= 0 && slotId < handler.slots.size())
            dropped = handler.getSlot(slotId).getStack();
        else if (slotId == -999 && (action == SlotActionType.PICKUP || action == SlotActionType.QUICK_CRAFT))
            dropped = handler.getCursorStack();
        if (!dropped.isEmpty() && dropRefused(dropped)) ci.cancel();
    }
}
