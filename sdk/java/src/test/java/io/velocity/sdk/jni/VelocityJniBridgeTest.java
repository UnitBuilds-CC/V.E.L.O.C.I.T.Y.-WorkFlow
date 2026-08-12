package io.velocity.sdk.jni;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Java JNI bridge.
 * Note: These tests require the native velocity_jni library to be on the library path.
 * Without the native library, only structural tests can run.
 */
class VelocityJniBridgeTest {

    @Test
    void testBridgeClassExists() {
        // Verify the bridge class can be loaded
        VelocityJniBridge bridge = new VelocityJniBridge();
        assertNotNull(bridge);
    }

    @Test
    void testBridgeHasNativeMethods() throws NoSuchMethodException {
        // Verify all native methods are declared
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineCreate"));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineCreateWorkflow",
            long.class, long.class, long.class, long.class, long.class, int.class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineCompleteStep",
            long.class, long.class, int.class, byte[].class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineSignalWorkflow",
            long.class, long.class, long.class, byte[].class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineQueryWorkflow",
            long.class, long.class, long.class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineGetStatus",
            long.class, long.class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineCancelWorkflow",
            long.class, long.class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineFailWorkflow",
            long.class, long.class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineDestroy",
            long.class));
        assertNotNull(VelocityJniBridge.class.getDeclaredMethod("velocityEngineActiveCount",
            long.class));
    }

    @Test
    void testBridgeImplementsAutoCloseable() {
        VelocityJniBridge bridge = new VelocityJniBridge();
        assertTrue(bridge instanceof AutoCloseable);
    }

    @Test
    void testInitialHandleIsZero() {
        VelocityJniBridge bridge = new VelocityJniBridge();
        assertEquals(0, bridge.getEngineHandle());
    }
}
