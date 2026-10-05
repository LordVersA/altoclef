package adris.altoclef.mixins;

import adris.altoclef.io.IoBridge;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.network.ClientPlayNetworkHandler;
import net.minecraft.network.packet.s2c.play.ItemPickupAnimationS2CPacket;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** Who picked up which item entity, before the game removes it: how a give to a player is seen to have happened. */
@Mixin(ClientPlayNetworkHandler.class)
public class IoPickupMixin {
    @Inject(method = "onItemPickupAnimation", at = @At("HEAD"))
    private void io$pickup(ItemPickupAnimationS2CPacket packet, CallbackInfo info) {
        var world = MinecraftClient.getInstance().world;
        if (world == null) return;
        var item = world.getEntityById(packet.getEntityId());
        var collector = world.getEntityById(packet.getCollectorEntityId());
        if (item != null && collector != null) IoBridge.itemPickedUp(item, collector, packet.getStackAmount());
    }
}
