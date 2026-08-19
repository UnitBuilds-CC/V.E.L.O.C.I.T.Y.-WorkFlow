package io.velocity.sdk;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Scans packages and directories for workflow and activity classes with VELOCITY annotations.
 * <p>
 * This enables auto-apply functionality where workflows and activities are automatically
 * discovered and registered without manual registration calls.
 * <p>
 * Usage:
 * <pre>{@code
 * WorkflowScanner scanner = new WorkflowScanner();
 * List<Class<?>> workflows = scanner.scanPackage("com.example.workflows");
 * for (Class<?> workflow : workflows) {
 *     worker.registerWorkflow(workflow);
 * }
 * }</pre>
 */
public class WorkflowScanner {

    /**
     * Scan a package for classes with VELOCITY workflow annotations.
     *
     * @param packageName the package name to scan (e.g., "com.example.workflows")
     * @return list of classes found with @DurableWorkflow annotation
     */
    public List<Class<?>> scanPackage(String packageName) {
        List<Class<?>> classes = new ArrayList<>();
        String path = packageName.replace('.', '/');
        
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Enumeration<URL> resources = classLoader.getResources(path);
            
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                String protocol = resource.getProtocol();
                
                if ("file".equals(protocol)) {
                    File directory = new File(resource.toURI());
                    classes.addAll(scanDirectory(directory, packageName));
                } else if ("jar".equals(protocol)) {
                    String jarPath = resource.getPath().substring(5, resource.getPath().indexOf("!"));
                    classes.addAll(scanJar(jarPath, path));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan package: " + packageName, e);
        }
        
        return classes;
    }

    /**
     * Scan a directory for classes with VELOCITY workflow annotations.
     */
    private List<Class<?>> scanDirectory(File directory, String packageName) {
        List<Class<?>> classes = new ArrayList<>();
        
        if (!directory.exists()) {
            return classes;
        }
        
        File[] files = directory.listFiles();
        if (files == null) {
            return classes;
        }
        
        for (File file : files) {
            if (file.isDirectory()) {
                classes.addAll(scanDirectory(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                String className = packageName + "." + file.getName().substring(0, file.getName().length() - 6);
                try {
                    Class<?> clazz = Class.forName(className);
                    if (clazz.isAnnotationPresent(DurableWorkflow.class)) {
                        classes.add(clazz);
                    }
                } catch (ClassNotFoundException e) {
                    // Skip classes that can't be loaded
                }
            }
        }
        
        return classes;
    }

    /**
     * Scan a JAR file for classes with VELOCITY workflow annotations.
     */
    private List<Class<?>> scanJar(String jarPath, String packagePath) {
        List<Class<?>> classes = new ArrayList<>();
        
        try (JarFile jarFile = new JarFile(jarPath)) {
            Enumeration<JarEntry> entries = jarFile.entries();
            
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                
                if (entryName.startsWith(packagePath) && entryName.endsWith(".class")) {
                    String className = entryName.substring(0, entryName.length() - 6)
                            .replace('/', '.');
                    try {
                        Class<?> clazz = Class.forName(className);
                        if (clazz.isAnnotationPresent(DurableWorkflow.class)) {
                            classes.add(clazz);
                        }
                    } catch (ClassNotFoundException e) {
                        // Skip classes that can't be loaded
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan JAR: " + jarPath, e);
        }
        
        return classes;
    }

    /**
     * Scan multiple packages for workflow classes.
     *
     * @param packageNames array of package names to scan
     * @return list of all classes found with @DurableWorkflow annotation
     */
    public List<Class<?>> scanPackages(String... packageNames) {
        List<Class<?>> allClasses = new ArrayList<>();
        for (String packageName : packageNames) {
            allClasses.addAll(scanPackage(packageName));
        }
        return allClasses;
    }
}
