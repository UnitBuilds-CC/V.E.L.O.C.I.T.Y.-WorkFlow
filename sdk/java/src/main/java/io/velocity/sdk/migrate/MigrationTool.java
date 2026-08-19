package io.velocity.sdk.migrate;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

/**
 * Velocity Java Migration Tool
 *
 * Scans a Java codebase for Temporal, Restate, or DBOS workflow patterns
 * and converts them to Velocity Java SDK workflows.
 *
 * Usage:
 *   java io.velocity.sdk.migrate.MigrationTool --src ./my_project --from temporal
 *   java io.velocity.sdk.migrate.MigrationTool --src ./workflows --from auto
 *   java io.velocity.sdk.migrate.MigrationTool --detect ./my_project
 */
public class MigrationTool {

    // ─── Pattern Definition ──────────────────────────────────────────────────

    static class MigrationPattern {
        final String name;
        final Pattern sourcePattern;
        final String targetTemplate;
        final String sourceFramework;

        MigrationPattern(String name, String regex, String target, String framework) {
            this.name = name;
            this.sourcePattern = Pattern.compile(regex);
            this.targetTemplate = target;
            this.sourceFramework = framework;
        }
    }

    // ─── Temporal → Velocity Patterns ────────────────────────────────────────

    static final List<MigrationPattern> TEMPORAL_PATTERNS = List.of(
        // Import replacements
        new MigrationPattern("temporal-import-workflow",
            "import\\s+io\\.temporal\\.workflow\\.",
            "import io.velocity.sdk.", "temporal"),
        new MigrationPattern("temporal-import-activity",
            "import\\s+io\\.temporal\\.activity\\.",
            "import io.velocity.sdk.", "temporal"),
        new MigrationPattern("temporal-import-client",
            "import\\s+io\\.temporal\\.client\\.",
            "import io.velocity.sdk.client.", "temporal"),
        new MigrationPattern("temporal-import-worker",
            "import\\s+io\\.temporal\\.worker\\.",
            "import io.velocity.sdk.worker.", "temporal"),

        // Annotation replacements
        new MigrationPattern("temporal-workflow-method",
            "@WorkflowMethod",
            "@WorkflowMethod", "temporal"),
        new MigrationPattern("temporal-signal-method",
            "@SignalMethod",
            "@SignalMethod", "temporal"),
        new MigrationPattern("temporal-query-method",
            "@QueryMethod",
            "@QueryMethod", "temporal"),
        new MigrationPattern("temporal-activity-method",
            "@ActivityMethod",
            "@ActivityMethod", "temporal"),

        // Method call replacements
        new MigrationPattern("temporal-activity-stub",
            "Workflow\\.newActivityStub\\(\\s*(\\w+)\\.class\\s*\\)",
            "WorkflowContext.newActivityStub($1.class)", "temporal"),
        new MigrationPattern("temporal-child-stub",
            "Workflow\\.newChildWorkflowStub\\(\\s*(\\w+)\\.class\\s*\\)",
            "WorkflowContext.newChildWorkflowStub($1.class)", "temporal"),
        new MigrationPattern("temporal-sleep",
            "Workflow\\.sleep\\(",
            "WorkflowContext.sleep(", "temporal"),
        new MigrationPattern("temporal-side-effect",
            "Workflow\\.sideEffect\\(",
            "WorkflowContext.sideEffect(", "temporal"),
        new MigrationPattern("temporal-random",
            "Workflow\\.newRandom\\(\\)",
            "WorkflowContext.newRandom()", "temporal"),
        new MigrationPattern("temporal-timer",
            "Workflow\\.newTimer\\(",
            "WorkflowContext.newTimer(", "temporal"),

        // Client/Worker
        new MigrationPattern("temporal-client-new",
            "TemporalServiceClient\\.newInstance\\(",
            "VelocityClient.create(", "temporal"),
        new MigrationPattern("temporal-worker-new",
            "WorkerFactory\\.newInstance\\(",
            "VelocityWorker.create(", "temporal"),
        new MigrationPattern("temporal-execute-workflow",
            "client\\.newWorkflowStub\\(\\s*(\\w+)\\.class",
            "client.newWorkflowStub($1.class", "temporal")
    );

    // ─── Restate → Velocity Patterns ─────────────────────────────────────────

    static final List<MigrationPattern> RESTATE_PATTERNS = List.of(
        new MigrationPattern("restate-import",
            "import\\s+dev\\.restate\\.sdk\\.",
            "import io.velocity.sdk.", "restate"),
        new MigrationPattern("restate-service",
            "import\\s+dev\\.restate\\.sdk\\.annotation\\.",
            "import io.velocity.sdk.annotation.", "restate"),
        new MigrationPattern("restate-context-run",
            "context\\.run\\(",
            "ctx.executeActivity(", "restate"),
        new MigrationPattern("restate-context-call",
            "context\\.call\\(",
            "ctx.executeActivity(", "restate"),
        new MigrationPattern("restate-context-sleep",
            "context\\.sleep\\(",
            "ctx.sleep(", "restate"),
        new MigrationPattern("restate-context-get",
            "context\\.get\\(",
            "ctx.getState(", "restate"),
        new MigrationPattern("restate-context-set",
            "context\\.set\\(",
            "ctx.setState(", "restate")
    );

    // ─── DBOS → Velocity Patterns ────────────────────────────────────────────

    static final List<MigrationPattern> DBOS_PATTERNS = List.of(
        new MigrationPattern("dbos-import",
            "import\\s+com\\.dbos\\.",
            "import io.velocity.sdk.", "dbos"),
        new MigrationPattern("dbos-workflow",
            "@DBOS\\.Workflow",
            "@WorkflowMethod", "dbos"),
        new MigrationPattern("dbos-transaction",
            "@DBOS\\.Transaction",
            "@ActivityMethod", "dbos"),
        new MigrationPattern("dbos-sleep",
            "DBOS\\.sleep\\(",
            "WorkflowContext.sleep(", "dbos"),
        new MigrationPattern("dbos-recv",
            "DBOS\\.recv\\(",
            "WorkflowContext.recv(", "dbos"),
        new MigrationPattern("dbos-set-event",
            "DBOS\\.setEvent\\(",
            "WorkflowContext.setEvent(", "dbos"),
        new MigrationPattern("dbos-get-event",
            "DBOS\\.getEvent\\(",
            "WorkflowContext.getEvent(", "dbos")
    );

    // ─── Framework Detection ─────────────────────────────────────────────────

    public static class DetectionResult {
        public final String framework;
        public final double confidence;
        public final List<String> evidence;

        public DetectionResult(String framework, double confidence, List<String> evidence) {
            this.framework = framework;
            this.confidence = confidence;
            this.evidence = evidence;
        }
    }

    public static DetectionResult detectFramework(String content) {
        Map<String, Integer> scores = new HashMap<>();
        scores.put("temporal", 0);
        scores.put("restate", 0);
        scores.put("dbos", 0);
        Map<String, List<String>> evidence = new HashMap<>();
        evidence.put("temporal", new ArrayList<>());
        evidence.put("restate", new ArrayList<>());
        evidence.put("dbos", new ArrayList<>());

        // Temporal checks
        if (content.contains("io.temporal.workflow")) {
            scores.merge("temporal", 3, Integer::sum);
            evidence.get("temporal").add("Temporal workflow import");
        }
        if (content.contains("io.temporal.activity")) {
            scores.merge("temporal", 3, Integer::sum);
            evidence.get("temporal").add("Temporal activity import");
        }
        if (content.contains("@WorkflowMethod")) {
            scores.merge("temporal", 2, Integer::sum);
            evidence.get("temporal").add("@WorkflowMethod annotation");
        }
        if (content.contains("Workflow.newActivityStub")) {
            scores.merge("temporal", 2, Integer::sum);
            evidence.get("temporal").add("Workflow.newActivityStub");
        }
        if (content.contains("Workflow.sleep")) {
            scores.merge("temporal", 1, Integer::sum);
            evidence.get("temporal").add("Workflow.sleep");
        }

        // Restate checks
        if (content.contains("dev.restate.sdk")) {
            scores.merge("restate", 3, Integer::sum);
            evidence.get("restate").add("Restate SDK import");
        }
        if (content.contains("context.run(")) {
            scores.merge("restate", 1, Integer::sum);
            evidence.get("restate").add("context.run()");
        }

        // DBOS checks
        if (content.contains("com.dbos")) {
            scores.merge("dbos", 3, Integer::sum);
            evidence.get("dbos").add("DBOS SDK import");
        }
        if (content.contains("@DBOS.Workflow") || content.contains("@DBOS")) {
            scores.merge("dbos", 2, Integer::sum);
            evidence.get("dbos").add("@DBOS annotation");
        }

        // Find best match
        String best = "temporal";
        int bestScore = 0;
        for (var entry : scores.entrySet()) {
            if (entry.getValue() > bestScore) {
                best = entry.getKey();
                bestScore = entry.getValue();
            }
        }

        int total = scores.values().stream().mapToInt(Integer::intValue).sum();
        double confidence = total > 0 ? (double) bestScore / total : 0.0;

        return new DetectionResult(best, confidence, evidence.get(best));
    }

    // ─── File Migration ──────────────────────────────────────────────────────

    public static class FileResult {
        public String sourcePath;
        public String outputPath;
        public boolean success;
        public String error;
        public String detectedFramework;
        public int transformations;
    }

    public static String[] migrateFile(String content, String sourceFramework) {
        FileResult result = new FileResult();
        result.success = true;

        // Auto-detect if needed
        if ("auto".equals(sourceFramework)) {
            DetectionResult detection = detectFramework(content);
            result.detectedFramework = detection.framework;
            if (detection.confidence < 0.3) {
                result.success = false;
                result.error = "Low confidence: " + detection.framework + " (" + detection.confidence + ")";
                return new String[]{content, "FAIL:" + result.error};
            }
            sourceFramework = detection.framework;
        } else {
            result.detectedFramework = sourceFramework;
        }

        // Select patterns
        List<MigrationPattern> patterns;
        switch (sourceFramework) {
            case "temporal": patterns = TEMPORAL_PATTERNS; break;
            case "restate": patterns = RESTATE_PATTERNS; break;
            case "dbos": patterns = DBOS_PATTERNS; break;
            default: return new String[]{content, "FAIL:Unknown framework: " + sourceFramework};
        }

        // Apply transformations
        String migrated = content;
        int count = 0;
        for (MigrationPattern p : patterns) {
            String newText = p.sourcePattern.matcher(migrated).replaceAll(p.targetTemplate);
            if (!newText.equals(migrated)) {
                count++;
                migrated = newText;
            }
        }

        return new String[]{migrated, "OK:" + count + ":" + result.detectedFramework};
    }

    // ─── Project Scanner ─────────────────────────────────────────────────────

    static final Set<String> SKIP_DIRS = Set.of(
        "target", "build", ".gradle", ".idea", "node_modules", ".git"
    );

    public static List<Path> scanJavaFiles(Path rootDir) throws IOException {
        try (Stream<Path> walk = Files.walk(rootDir)) {
            return walk
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".java"))
                .filter(p -> !p.toString().contains("target" + File.separator))
                .collect(Collectors.toList());
        }
    }

    public static boolean hasWorkflowContent(String content) {
        String[] indicators = {
            "io.temporal", "dev.restate", "com.dbos",
            "@WorkflowMethod", "@ActivityMethod",
            "Workflow.newActivityStub", "Workflow.sleep",
            "@DBOS", "context.run(",
        };
        for (String ind : indicators) {
            if (content.contains(ind)) return true;
        }
        return false;
    }

    // ─── Main ────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String src = null;
        String from = "auto";
        String output = null;
        boolean dryRun = false;
        boolean detect = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--src": src = args[++i]; break;
                case "--from": from = args[++i]; break;
                case "--output": case "-o": output = args[++i]; break;
                case "--dry-run": dryRun = true; break;
                case "--detect": detect = true; break;
                case "--help": case "-h":
                    System.out.println("Velocity Java Migration Tool");
                    System.out.println("Usage:");
                    System.out.println("  --src <file|dir>     Source file or directory");
                    System.out.println("  --from <framework>   Source: temporal, restate, dbos, auto");
                    System.out.println("  --output <path>      Output file or directory");
                    System.out.println("  --dry-run            Detect without writing");
                    System.out.println("  --detect             Detect framework in directory");
                    return;
            }
        }

        if (src == null) {
            System.err.println("Error: --src is required");
            System.exit(1);
        }

        Path srcPath = Path.of(src);

        // Mode: detect
        if (detect) {
            if (!Files.isDirectory(srcPath)) {
                System.err.println("Error: --detect requires a directory");
                System.exit(1);
            }
            List<Path> files = scanJavaFiles(srcPath);
            System.out.printf("Scanning %d Java files in %s...%n", files.size(), src);
            for (Path f : files) {
                String content = Files.readString(f);
                DetectionResult result = detectFramework(content);
                if (result.confidence > 0.3) {
                    Path rel = srcPath.relativize(f);
                    System.out.printf("  %s: %s (%.0f%%) [%s]%n",
                        rel, result.framework, result.confidence * 100,
                        String.join(", ", result.evidence));
                }
            }
            return;
        }

        // Mode: single file
        if (Files.isRegularFile(srcPath)) {
            String content = Files.readString(srcPath);
            String[] result = migrateFile(content, from);

            if (result[1].startsWith("FAIL:")) {
                System.err.println("Migration failed: " + result[1].substring(5));
                System.exit(1);
            }

            if (output != null) {
                Files.writeString(Path.of(output), result[0]);
                System.out.println("Written to: " + output);
            } else {
                System.out.print(result[0]);
            }

            String[] parts = result[1].split(":");
            System.out.println("\nDetected: " + parts[2]);
            System.out.println("Transformations: " + parts[1]);
            return;
        }

        // Mode: directory
        String outputDir = output != null ? output : srcPath.getParent().resolve("velocity-migrated").toString();
        System.out.printf("Scanning: %s%n", src);
        System.out.printf("Output: %s%n%n", dryRun ? "(dry run)" : outputDir);
        System.out.printf("Source framework: %s%n%n", from);

        List<Path> files = scanJavaFiles(srcPath);
        int migrated = 0, failed = 0, skipped = 0;

        for (Path f : files) {
            String content = Files.readString(f);
            if (!hasWorkflowContent(content)) { skipped++; continue; }

            String[] result = migrateFile(content, from);
            if (result[1].startsWith("FAIL:")) {
                failed++;
                System.out.printf("  [FAIL] %s: %s%n", srcPath.relativize(f), result[1].substring(5));
            } else {
                migrated++;
                if (!dryRun) {
                    Path rel = srcPath.relativize(f);
                    Path outPath = Path.of(outputDir).resolve(rel);
                    Files.createDirectories(outPath.getParent());
                    Files.writeString(outPath, result[0]);
                }
                String[] parts = result[1].split(":");
                System.out.printf("  [OK] %s (%s, %s changes)%n",
                    srcPath.relativize(f), parts[2], parts[1]);
            }
        }

        System.out.printf("%nResults: %d files, %d migrated, %d failed, %d skipped%n",
            files.size(), migrated, failed, skipped);
    }
}
