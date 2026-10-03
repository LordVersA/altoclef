package adris.altoclef.io;

import adris.altoclef.eventbus.EventBus;
import adris.altoclef.eventbus.events.ClientTickEvent;
import net.minecraft.client.MinecraftClient;
import org.lwjgl.glfw.GLFW;

/** Optional unattended-client window handling, executed on the client thread after joining. */
public final class IoClientWindow {
    private static boolean minimized;

    private IoClientWindow() {}

    public static void start() {
        if (!"1".equals(System.getenv("IO_CLIENT_MINIMIZE"))) return;
        EventBus.subscribe(ClientTickEvent.class, evt -> {
            MinecraftClient mc = MinecraftClient.getInstance();
            if (minimized || mc.player == null || mc.world == null) return;
            long handle = mc.getWindow().getHandle();
            GLFW.glfwIconifyWindow(handle);
            minimized = GLFW.glfwGetWindowAttrib(handle, GLFW.GLFW_ICONIFIED) == GLFW.GLFW_TRUE;
            System.out.println("IO-CLIENT: minimized=" + minimized);
        });
    }
}
