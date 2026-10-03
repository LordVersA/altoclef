package adris.altoclef.mixins;

import adris.altoclef.eventbus.EventBus;
import adris.altoclef.eventbus.events.TitleScreenEntryEvent;
import net.minecraft.client.network.ClientPlayNetworkHandler;
import net.minecraft.network.packet.s2c.play.GameJoinS2CPacket;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * IO: AltoClef starts from the title screen (see EntryMixin). A client launched straight into a
 * world or server (quick play, which is how an unattended client joins) never shows the title
 * screen, so nothing was initialised and opening chat crashed on a null command executor.
 * Joining a world publishes the same event; AltoClef.onInitializeLoad ignores it when it has
 * already run. TAIL, because the packet is handled on the main thread only after it is re-queued.
 */
@Mixin(ClientPlayNetworkHandler.class)
public class JoinInitMixin {
    @Inject(at = @At("TAIL"), method = "onGameJoin")
    private void altoclef$initOnJoin(GameJoinS2CPacket packet, CallbackInfo info) {
        EventBus.publish(new TitleScreenEntryEvent());
    }
}
