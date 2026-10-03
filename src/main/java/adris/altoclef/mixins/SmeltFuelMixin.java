package adris.altoclef.mixins;

import adris.altoclef.Settings;
import adris.altoclef.io.IoBridge;
import net.minecraft.item.Item;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/** Honor a caller-selected smelting fuel without changing the persisted companion defaults. */
@Mixin(value = Settings.class, remap = false)
public abstract class SmeltFuelMixin {
    @Inject(method = "isSupportedFuel", at = @At("HEAD"), cancellable = true, remap = false)
    private void io$selectedFuel(Item item, CallbackInfoReturnable<Boolean> info) {
        Item selected = IoBridge.selectedSmeltFuel();
        if (selected != null) info.setReturnValue(item == selected);
    }
    @Inject(method = "getSupportedFuelItems", at = @At("HEAD"), cancellable = true, remap = false)
    private void io$selectedFuelList(CallbackInfoReturnable<Item[]> info) {
        Item selected = IoBridge.selectedSmeltFuel();
        if (selected != null) info.setReturnValue(new Item[]{selected});
    }
}
