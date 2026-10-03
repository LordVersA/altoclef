package adris.altoclef.mixins;

import adris.altoclef.io.IoBridge;
import net.minecraft.client.network.ClientPlayNetworkHandler;
import net.minecraft.network.packet.s2c.play.InventoryS2CPacket;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** Do not claim an opened container is empty before the server supplies its slots. */
@Mixin(ClientPlayNetworkHandler.class)
public class IoInventoryReceiptMixin {
    @Inject(method = "onInventory", at = @At("TAIL"))
    private void io$receivedInventory(InventoryS2CPacket packet, CallbackInfo info) {
        IoBridge.receivedInventory(packet.getSyncId());
    }
}
