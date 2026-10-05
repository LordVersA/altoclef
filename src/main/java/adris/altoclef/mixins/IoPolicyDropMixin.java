package adris.altoclef.mixins;

import adris.altoclef.io.IoBridge;
import adris.altoclef.io.IoPolicy;
import net.minecraft.client.network.ClientPlayerEntity;
import net.minecraft.item.ItemStack;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/** The drop key and `dropItem` on the player itself: the other way an item leaves the inventory (plan 6.7). */
@Mixin(ClientPlayerEntity.class)
public final class IoPolicyDropMixin {
    @Inject(method = "dropSelectedItem", at = @At("HEAD"), cancellable = true)
    private void io$dropSelected(boolean entireStack, CallbackInfoReturnable<Boolean> cir) {
        IoPolicy policy = IoPolicy.current();
        if (policy == null || IoBridge.dropAuthorizedNow()) return;
        ItemStack held = ((ClientPlayerEntity) (Object) this).getMainHandStack();
        String refusal = policy.dropRefusal(held);
        if (refusal == null) return;
        IoBridge.policyBlocked("drop", refusal, net.minecraft.registry.Registries.ITEM.getId(held.getItem()).getPath());
        cir.setReturnValue(false);
    }
}
