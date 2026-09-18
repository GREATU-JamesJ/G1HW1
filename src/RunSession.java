import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Local presentation and recording only; no change to the wire protocol or simulation clock. */
class RunSession {
    static RunSession current;
    final Path directory;
    private final String host;
    private final int port;
    private final Instant started = Instant.now();
    private final long startNanos = System.nanoTime();
    private final Set<String> successes = new HashSet<>();
    private final Set<String> connected = new HashSet<>();
    private final Set<String> terminated = new HashSet<>();
    private final List<String> errors = new ArrayList<>();
    private final ScheduledExecutorService progress = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "console-progress"); t.setDaemon(true); return t;
    });
    private final Thread shutdownHook;
    private boolean verbose;
    private boolean finished;
    private int successEvents;
    private int failures;
    private int overflows;
    private long clock;
    private String lastProgress = "";

    RunSession(String host, int port, boolean verbose) throws IOException {
        this.host = host;
        this.port = port;
        this.verbose = verbose;
        Path base = Path.of("runs").toAbsolutePath();
        Files.createDirectories(base);
        String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        directory = Files.createDirectory(base.resolve(stamp + "-" + UUID.randomUUID()));
        writeMetadata("RUNNING", "실행 중", null);
        shutdownHook = new Thread(() -> {
            synchronized (this) {
                if (finished) return;
                progress.shutdownNow();
                try {
                    String summary = "실행 중단: 완료 여부를 검증하지 못했습니다.\n로그: " + directory + "\n";
                    Files.writeString(directory.resolve("summary.txt"), summary);
                    writeMetadata("ABORTED", "종료 신호로 중단됨", Instant.now());
                } catch (IOException e) {
                    System.err.println("중단 상태 저장 실패: " + e.getMessage());
                }
            }
        }, "run-status-save");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    static Path output(String name) {
        return current == null ? Path.of(name) : current.directory.resolve(name);
    }

    void start() {
        System.out.println("분산 KV 처리 | Master " + host + ":" + port);
        System.out.println("로그 폴더: " + directory);
        System.out.println("상세 출력: " + (verbose ? "ON" : "OFF") + " | v + Enter로 전환 (파일에는 항상 전체 기록)");
        progress.scheduleAtFixedRate(this::showProgress, 0, 500, TimeUnit.MILLISECONDS);
        Thread input = new Thread(() -> {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (this) {
                        if (finished) return;
                        if ("v".equalsIgnoreCase(line.trim())) {
                            verbose = !verbose;
                            System.out.println("상세 출력: " + (verbose ? "ON" : "OFF"));
                        }
                    }
                }
            } catch (IOException ignored) { /* Interactive input is optional. */ }
        }, "console-mode");
        input.setDaemon(true);
        input.start();
    }

    synchronized void event(long time, String node, String event, String status, String message, String line) {
        clock = Math.max(clock, time);
        if (event.equals("CONNECT") && status.equals("SUCCESS")) connected.add(node);
        if (event.equals("TERMINATE") && status.equals("SUCCESS")) terminated.add(node);
        if (event.equals("QUEUE") && status.equals("FAIL")) overflows++;
        if (event.equals("PROC") && status.equals("SUCCESS")) {
            Matcher id = Pattern.compile("KV\\[(\\d+)\\]").matcher(message);
            if (id.find()) { successes.add(id.group(1)); successEvents++; }
        }
        if (event.equals("PROC") && status.equals("FAIL")) failures++;
        if (!finished && verbose) System.out.println(line);
    }

    synchronized void error(String message) {
        if (!errors.contains(message)) errors.add(message);
        if (!finished) System.err.println("[실행 오류] " + message);
    }

    private synchronized void showProgress() {
        if (finished || verbose) return;
        String state = "연결 " + connected.size() + "/4 | 고유 성공 " + successes.size()
                + "/5000 | 실패 보고 " + failures + " | 가상 " + VirtualClock.format(clock) + "초";
        if (!state.equals(lastProgress)) {
            System.out.println(state);
            lastProgress = state;
        }
    }

    synchronized int finish() throws IOException {
        progress.shutdownNow();
        String master = Files.exists(directory.resolve("Master.txt"))
                ? Files.readString(directory.resolve("Master.txt")) : "";
        Set<String> masterSuccess = new HashSet<>();
        Matcher ids = Pattern.compile("\\| RESULT \\| SUCCESS \\| KV\\[(\\d+)\\]").matcher(master);
        int masterSuccessEvents = 0;
        while (ids.find()) { masterSuccess.add(ids.group(1)); masterSuccessEvents++; }
        boolean allIds = true;
        for (int i = 1; i <= 5000; i++) allIds &= masterSuccess.contains(String.format(Locale.ROOT, "%04d", i));
        boolean masterComplete = master.contains("| TERMINATE | SUCCESS")
                && number(master, "KV 처리 완료 수: (\\d+)") == 5000
                && masterSuccessEvents == 5000 && masterSuccess.size() == 5000 && allIds;
        boolean workerComplete = terminated.size() == 4 && successes.size() == 5000 && successEvents == 5000;
        boolean matched = successes.equals(masterSuccess);
        String status = !errors.isEmpty() || !workerComplete ? "FAILED"
                : masterComplete && matched ? "COMPLETED" : "PARTIAL";
        String description = status.equals("COMPLETED") ? "정상 완료 (고유 작업 5,000개 및 종료 기록 확인)"
                : status.equals("PARTIAL") ? "부분 확인: Master 로그 수신 또는 집계 검증 필요" : "실패 또는 미완료";
        int p2p = number(master, "P2P 부하 분산 수: (\\d+)");
        int transferred = 0;
        Matcher batches = Pattern.compile("작업 전송, 대상 워커\\d+, 수량=(\\d+)").matcher(master);
        while (batches.find()) transferred += Integer.parseInt(batches.group(1));
        int retries = number(master, "장애 재할당 수: (\\d+)");
        StringBuilder text = new StringBuilder("\n========== 최종 결과 ==========\n");
        text.append(description).append('\n');
        text.append("고유 성공: ").append(successes.size()).append("/5000 | 중복 성공 보고: ")
                .append(successEvents - successes.size()).append('\n');
        text.append("실패 보고: ").append(failures).append(" | Queue 초과 거부: ").append(overflows)
                .append(" | Master 재할당: ").append(retries < 0 ? "확인 불가" : retries).append('\n');
        text.append("P2P 이벤트: ").append(p2p < 0 ? "확인 불가" : p2p + "회")
                .append(p2p < 0 ? "" : " / 이전 작업 " + transferred + "건")
                .append(p2p == 0 ? " (이번 실행에서 발생하지 않음; 별도 검증 필요)" : "").append('\n');
        text.append("Worker   성공   처리실패   P2P전송/수신(건)   평균대기(가상초)\n");
        for (int i = 1; i <= 4; i++) {
            Path file = directory.resolve("Worker" + i + ".txt");
            String data = Files.exists(file) ? Files.readString(file) : "";
            text.append(String.format(Locale.ROOT, "%d        %s   %s       %s           %s%n", i,
                    extract(data, "\\| STAT \\| SUCCESS \\| 성공: (\\d+)"),
                    extract(data, "\\| STAT \\| FAIL \\| 실패: (\\d+)"),
                    extract(data, "P2P 작업 전송/수신: (\\d+ / \\d+)"),
                    extract(data, "평균 대기 시간: ([\\d.]+)초")));
        }
        text.append(String.format(Locale.ROOT, "실제 실행: %.2f초 | 가상 수행: %s초%n", elapsed(), VirtualClock.format(clock)));
        text.append("Master 로그 검증: ").append(masterComplete && matched ? "완료" : "확인 불가/불일치").append('\n');
        for (String error : errors) text.append("오류: ").append(error).append('\n');
        text.append("로그 및 요약: ").append(directory).append('\n');
        Files.writeString(directory.resolve("summary.txt"), text.toString());
        writeMetadata(status, description, Instant.now());
        finished = true;
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        System.out.print(text);
        return status.equals("COMPLETED") ? 0 : status.equals("PARTIAL") ? 2 : 1;
    }

    private double elapsed() { return (System.nanoTime() - startNanos) / 1_000_000_000.0; }
    private static String extract(String text, String pattern) {
        Matcher m = Pattern.compile(pattern).matcher(text);
        return m.find() ? m.group(1) : "-";
    }
    private static int number(String text, String pattern) {
        String value = extract(text, pattern);
        return value.equals("-") ? -1 : Integer.parseInt(value);
    }
    private static String json(String value) {
        if (value == null) return "null";
        StringBuilder result = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') result.append('\\').append(c);
            else if (c < 32) result.append(String.format(Locale.ROOT, "\\u%04x", (int)c));
            else result.append(c);
        }
        return result.append('"').toString();
    }
    private void writeMetadata(String status, String description, Instant ended) throws IOException {
        String data = "{\n  \"runId\": " + json(directory.getFileName().toString())
                + ",\n  \"masterHost\": " + json(host) + ",\n  \"masterPort\": " + port
                + ",\n  \"startedAt\": " + json(started.toString())
                + ",\n  \"endedAt\": " + json(ended == null ? null : ended.toString())
                + ",\n  \"status\": " + json(status) + ",\n  \"description\": " + json(description)
                + ",\n  \"elapsedSeconds\": " + String.format(Locale.ROOT, "%.3f", elapsed())
                + ",\n  \"virtualSeconds\": " + VirtualClock.format(clock)
                + ",\n  \"uniqueSuccesses\": " + successes.size() + "\n}\n";
        Path temp = directory.resolve("run.json.tmp");
        Files.writeString(temp, data, StandardCharsets.UTF_8);
        Files.move(temp, directory.resolve("run.json"), StandardCopyOption.REPLACE_EXISTING);
    }
}
