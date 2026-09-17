import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

class MasterNode {
    private static final int WORKER_COUNT = 4;
    private static final int TASK_COUNT = 5000;
    private static final int QUEUE_LIMIT = 10;

    private final int port;
    private final Object lock = new Object();
    private final VirtualClock clock = new VirtualClock();
    private final Map<Integer, WorkerInfo> workers = new HashMap<>();
    private final Map<String, Integer> kvStore = new HashMap<>();
    private final Map<String, Task> allTasks = new HashMap<>();
    private final ArrayDeque<Task> pending = new ArrayDeque<>();
    private final PriorityQueue<Task> retries = new PriorityQueue<>();
    private final CountDownLatch terminationAcks = new CountDownLatch(WORKER_COUNT);
    private EventLogger log;
    private int completed;
    private int totalSuccess;
    private int totalFail;
    private int reassignmentCount;
    private int p2pEvents;
    private int nextProgress = 500;
    private int tieCursor;
    private boolean terminating;

    // Master 포트 설정
    MasterNode(int port) {
        this.port = port;
    }

    // Master 전체 실행
    void run() throws Exception {
        try (EventLogger eventLog = new EventLogger("Master.txt");
             ServerSocket server = new ServerSocket(port)) {
            log = eventLog;
            log.header("Master.txt (Master Node Log)", "Master Node | Distributed KV Store");
            write("INIT", "INFO", "시스템 시계 시작, 포트 " + port + " 연결 대기");
            generateTasks();
            acceptWorkers(server);
            startReaders();
            synchronized (lock) {
                write("INIT", "SUCCESS", "워커 4개 연결 완료, 작업 배정 시작");
                dispatchAvailableTasks();
            }
            terminationAcks.await();
            synchronized (lock) {
                writeFinalStatistics();
                write("TERMINATE", "SUCCESS", "정상 종료 완료");
                sendMasterLog();
            }
        }
    }

    // 고유 KV 작업 5,000개 생성
    private void generateTasks() {
        Set<String> usedKeys = new HashSet<>();
        for (int number = 1; number <= TASK_COUNT; number++) {
            String key;
            do {
                key = String.format("%04x", ThreadLocalRandom.current().nextInt(0x10000));
            } while (!usedKeys.add(key));
            Task task = new Task(String.format("%04d", number), key,
                    ThreadLocalRandom.current().nextInt(1, 101), 0, false, 0);
            pending.addLast(task);
            allTasks.put(task.id, task);
        }
        write("INIT", "SUCCESS", "고유 KV 작업 5,000개 생성 완료");
    }

    // Worker 4개 연결 수락
    private void acceptWorkers(ServerSocket server) throws IOException {
        while (workers.size() < WORKER_COUNT) {
            Socket socket = server.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String hello = in.readLine();
            String[] p = hello == null ? new String[0] : hello.split("\\|");
            if (p.length != 3 || !"HELLO".equals(p[0])) {
                socket.close();
                continue;
            }
            int id = Integer.parseInt(p[1]);
            if (id < 1 || id > WORKER_COUNT || workers.containsKey(id)) {
                out.println("REJECT|잘못된 워커 번호");
                socket.close();
                continue;
            }
            WorkerInfo worker = new WorkerInfo(id, Integer.parseInt(p[2]), socket, out);
            workers.put(id, worker);
            worker.send("WELCOME|" + clock.now());
            write("CONNECT", "SUCCESS", "워커" + id + " 연결, 대기열 초기화 (0/10)");
            Thread reader = new Thread(() -> readWorker(worker, in), "master-reader-" + id);
            reader.setDaemon(true);
            reader.start();
        }
    }

    // Worker 수신 Thread 시작 상태 유지
    private void startReaders() {
        // Worker 연결 수락 과정에서 시작 완료
    }

    // Worker 메시지 반복 수신
    private void readWorker(WorkerInfo worker, BufferedReader in) {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                handleMessage(worker, line);
            }
        } catch (IOException ignored) {
            synchronized (lock) {
                worker.connected = false;
                write("CONNECT", "FAIL", "워커" + worker.id + " 연결 해제");
            }
        }
    }

    // Worker 메시지 유형별 처리
    private void handleMessage(WorkerInfo worker, String line) {
        String[] p = line.split("\\|", -1);
        synchronized (lock) {
            switch (p[0]) {
                case "STATUS" -> worker.queueSize = Integer.parseInt(p[1]);
                case "RESULT" -> handleResult(worker, p);
                case "P2P" -> handleP2p(worker, p);
                case "LOG_REQUEST" -> worker.logRequested = true;
                case "TERMINATE_ACK" -> terminationAcks.countDown();
                default -> write("PROTO", "WARN", "워커" + worker.id + " 알 수 없는 메시지: " + line);
            }
            if (!terminating) {
                dispatchAvailableTasks();
                checkCompletion();
            }
        }
    }

    // 작업 성공·실패 결과 처리
    private void handleResult(WorkerInfo worker, String[] p) {
        Task task = allTasks.get(p[1]);
        if (task == null) return;
        String result = p[2];
        double duration = Double.parseDouble(p[3]);
        worker.queueSize = Integer.parseInt(p[5]);
        clock.advanceSeconds(duration + 1.0);
        worker.processed++;
        if ("SUCCESS".equals(result)) {
            worker.success++;
            totalSuccess++;
            completed++;
            kvStore.put(task.key, task.value);
            write("RESULT", "SUCCESS", "KV[" + task.id + "] 워커" + worker.id
                    + " 저장 완료, 값=" + task.value + ", 처리=" + p[3] + "초");
            writeProgressIfNeeded();
        } else {
            worker.fail++;
            totalFail++;
            retries.offer(task.retryCopy());
            reassignmentCount++;
            write("RESULT", "FAIL", "KV[" + task.id + "] 워커" + worker.id
                    + " 처리 실패, 우선 재시도 Queue 등록");
        }
        worker.send("RESULT_ACK|" + task.id + "|" + result + "|" + clock.now());
    }

    // P2P 이전 결과 처리
    private void handleP2p(WorkerInfo worker, String[] p) {
        String direction = p[1];
        int count = Integer.parseInt(p[3]);
        String taskIds = p.length >= 5 ? p[4] : "";
        clock.advanceSeconds(1.0);
        if ("SENT".equals(direction)) {
            worker.p2pSent += count;
            p2pEvents++;
        } else {
            worker.p2pReceived += count;
        }
        String action = "SENT".equals(direction) ? "작업 전송" : "작업 수신";
        write("LB", "SUCCESS", "워커" + worker.id + " " + action
                + ", 대상 워커" + p[2] + ", 수량=" + count
                + (taskIds.isEmpty() ? "" : ", KV=" + taskIds));
        worker.send("P2P_ACK|" + direction + "|" + p[2] + "|" + count + "|"
                + taskIds + "|" + clock.now());
    }

    // 여유 Queue 대상 작업 배정
    private void dispatchAvailableTasks() {
        while (true) {
            WorkerInfo target = selectLeastLoadedWorker();
            if (target == null) return;
            Task task = !retries.isEmpty() ? retries.poll() : pending.pollFirst();
            if (task == null) return;
            task.enqueuedAt = clock.advanceSeconds(1.0);
            target.queueSize++;
            target.send("TASK|" + task.wire());
            String priority = task.retry ? "우선 재시도" : "일반";
            write("DISTRIB", "INFO", "KV[" + task.id + "] -> 워커" + target.id
                    + " 배정, 대기열=" + target.queueSize + "/10, " + priority);
        }
    }

    // 최소 Queue Worker 선택
    private WorkerInfo selectLeastLoadedWorker() {
        List<WorkerInfo> candidates = new ArrayList<>();
        int smallest = Integer.MAX_VALUE;
        for (WorkerInfo worker : workers.values()) {
            if (!worker.connected || worker.queueSize >= QUEUE_LIMIT) continue;
            if (worker.queueSize < smallest) {
                smallest = worker.queueSize;
                candidates.clear();
                candidates.add(worker);
            } else if (worker.queueSize == smallest) {
                candidates.add(worker);
            }
        }
        if (candidates.isEmpty()) return null;
        candidates.sort(Comparator.comparingInt(w -> w.id));
        WorkerInfo selected = candidates.get(tieCursor % candidates.size());
        tieCursor++;
        return selected;
    }

    // 전체 작업 완료 조건 확인
    private void checkCompletion() {
        // 모든 고유 KV가 성공 저장되면 남은 중복 재시도 항목은 더 처리하지 않는다.
        if (completed != TASK_COUNT) return;
        terminating = true;
        write("DISTRIB", "SUCCESS", "KV 작업 5,000개 처리 완료, 종료 신호 전송");
        for (WorkerInfo worker : workers.values()) worker.send("TERMINATE|" + clock.now());
    }

    // Master 최종 통계 기록
    private void writeFinalStatistics() {
        write("STAT", "INFO", "=== 최종 통계 ===");
        write("STAT", "INFO", "KV 처리 완료 수: " + kvStore.size());
        int totalAttempts = totalSuccess + totalFail;
        double successRate = totalAttempts == 0 ? 0.0 : totalSuccess * 100.0 / totalAttempts;
        double failRate = totalAttempts == 0 ? 0.0 : totalFail * 100.0 / totalAttempts;
        write("STAT", "SUCCESS", String.format("총 성공: %,d (%.1f%%)", totalSuccess, successRate));
        write("STAT", "FAIL", String.format("총 실패(재시도 전): %,d (%.1f%%)", totalFail, failRate));
        write("STAT", "INFO", "장애 재할당 수: " + reassignmentCount);
        write("STAT", "INFO", "P2P 부하 분산 수: " + p2pEvents);
        for (WorkerInfo worker : workers.values()) {
            write("STAT", "INFO", "워커" + worker.id + ": 처리=" + worker.processed
                    + ", 성공=" + worker.success + ", 실패=" + worker.fail
                    + ", P2P 전송=" + worker.p2pSent + ", 수신=" + worker.p2pReceived);
        }
        write("STAT", "INFO", "전체 수행 시간: " + VirtualClock.format(clock.now()) + "초");
    }

    // Worker1에 Master 로그 전송
    private void sendMasterLog() {
        WorkerInfo receiver = workers.get(1);
        if (receiver == null || !receiver.connected || !receiver.logRequested) return;
        try {
            byte[] content = Files.readAllBytes(Path.of("Master.txt"));
            receiver.send("MASTER_LOG_BEGIN|" + content.length);
            for (int offset = 0; offset < content.length; offset += 4096) {
                int size = Math.min(4096, content.length - offset);
                byte[] chunk = java.util.Arrays.copyOfRange(content, offset, offset + size);
                receiver.send("MASTER_LOG_CHUNK|" + java.util.Base64.getEncoder().encodeToString(chunk));
            }
            receiver.send("MASTER_LOG_END");
        } catch (IOException e) {
            // 로그 전송 실패는 Master 실행 로그에 기록하지 않는다.
        }
    }

    // Master 로그 출력
    private void write(String event, String status, String message) {
        log.log(clock.now(), "MASTER", event, status, message);
    }

    // 처리 완료 진행률 기록
    private void writeProgressIfNeeded() {
        if (completed < nextProgress && completed != TASK_COUNT) return;
        double rate = completed * 100.0 / TASK_COUNT;
        write("DISTRIB", "INFO", String.format("진행률: %d / %d (%.1f%%)",
                completed, TASK_COUNT, rate));
        while (nextProgress <= completed) nextProgress += 500;
    }
}
