package adris.altoclef.mixins;

import baritone.Baritone;
import baritone.utils.BaritoneProcessHelper;
import baritone.process.BuilderProcess;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** MiranCZ's builder patch updates the current movement even for a zero-movement path. */
@Mixin(value = BuilderProcess.class, remap = false)
public abstract class BuilderMovementMixin extends BaritoneProcessHelper {
    protected BuilderMovementMixin(Baritone baritone) {
        super(baritone);
    }

    @Inject(method = "updateMovement", at = @At("HEAD"), cancellable = true, remap = false)
    private void io$skipMissingMovement(CallbackInfo info) {
        var current = baritone.getPathingBehavior().getCurrent();
        if (current == null) return;
        int index = current.getPosition();
        if (index < 0 || index >= current.getPath().movements().size()) info.cancel();
    }
}
