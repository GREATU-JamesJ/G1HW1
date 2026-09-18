import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

class WorkerNode implements Runnable {
    private static final int QUEUE_LIMIT = 10;
    private final int id;
    private final String masterHost;
    private final int masterPort;
    private final int peerPort;
    private final Object queueLock = new Object();
    private final Deque<Task> queue = new ArrayDeque<>();
    private final Map<String, PendingResult> pendingResults = new ConcurrentHashMap<>();
    private final ByteArrayOutputStream masterLogBytes = new ByteArrayOutputStream();
    private final CountDownLatch masterLogReceived = new CountDownLatch(1);
    private volatile boolean stopping;
    private volatile boolean terminatedByMaster;
    private volatile boolean masterLogSaved;
    private volatile boolean closing;
    private ServerSocket peerServer;
    private Thread peerThread;
    private long masterClockMillis;
    private long workerAvailableAt;
    private long nextLoadCheck;
    private PrintWriter masterOut;
    private EventLogger log;
    private int success;
    private int fail;
    private int processed;
    private int received;
    private int p2pSent;
    private int p2pReceived;
    private double totalWait;

    // Worker 실행 정보 설정
    WorkerNode(int id, String masterHost, int masterPort, int peerPort) {
        this.id = id;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.peerPort = peerPort;
    }

    // Worker Thread 4개 시작
    static void startFour(String host, int port) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        for (int workerId = 1; workerId <= 4; workerId++) {
            Thread thread = new Thread(new WorkerNode(workerId, host, port, 6000 + workerId),
                    "worker-" + workerId);
            threads.add(thread);
            thread.setUncaughtExceptionHandler((t, e) -> RunSession.current.error(t.getName() + ": " + e));
            thread.start();
        }
        for (Thread thread : threads) thread.join();
    }

    // Worker 전체 실행
    @Override
    public void run() {
        try (EventLogger eventLog = new EventLogger("Worker" + id + ".txt")) {
            log = eventLog;
            log.header("Worker" + id + ".txt (Worker Node " + id + " Log)",
                    "Worker" + id + " | Thread-based Worker | Ready Queue max=10");
            writeAt(0, "INIT", "INFO", "워커 Thread 시작, 마스터 연결 시도");
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(masterHost, masterPort), 10000);
                socket.setSoTimeout(30000);
                startPeerListener();
                masterOut = new PrintWriter(socket.getOutputStream(), true);
                sendMaster("HELLO|" + id + "|" + peerPort);
                Thread receiver = new Thread(() -> receiveMaster(socket),
                        "worker-master-reader-" + id);
                receiver.setDaemon(true);
                receiver.start();
                try {
                    processLoop();
                    if (terminatedByMaster) {
                        if (id == 1) sendMaster("LOG_REQUEST");
                        sendMaster("TERMINATE_ACK");
                        waitForMasterLog();
                        // Let Master consume the ACK before closing a socket that may still have unread data.
                        if (id != 1) receiver.join(15000);
                    }
                } finally {
                    closing = true;
                    stopping = true;
                    socket.close();
                    if (peerServer != null) peerServer.close();
                    receiver.join();
                    if (peerThread != null) peerThread.join();
                }
                if (terminatedByMaster) writeFinalStatistics();
            } catch (IOException | InterruptedException | RuntimeException e) {
                stopping = true;
                if (peerServer != null) peerServer.close();
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                RunSession.current.error("Worker" + id + " 연결/처리 실패: " + e.getMessage());
                writeAt(masterClockMillis, "CONNECT", "FAIL", "연결/처리 실패: " + e.getMessage());
            }
        } catch (IOException e) {
            RunSession.current.error("Worker" + id + " 로그 파일 오류: " + e.getMessage());
        }
    }

    // Master 메시지 반복 수신
    private void receiveMaster(Socket socket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                String[] p = line.split("\\|", -1);
                switch (p[0]) {
                    case "WELCOME" -> {
                        updateMasterClock(Long.parseLong(p[1]));
                        write("CONNECT", "SUCCESS", "마스터 연결, 대기열 초기화 (0/10)");
                    }
                    case "TASK" -> receiveTask(Task.fromWire(joinFrom(p, 1)));
                    case "RESULT_ACK" -> receiveResultAck(p);
                    case "P2P_ACK" -> receiveP2pAck(p);
                    case "TERMINATE" -> {
                        terminatedByMaster = true;
                        if (p.length > 1) updateMasterClock(Long.parseLong(p[1]));
                        synchronized (queueLock) {
                            stopping = true;
                            queueLock.notifyAll();
                        }
                    }
                    case "MASTER_LOG_BEGIN" -> masterLogBytes.reset();
                    case "MASTER_LOG_CHUNK" -> receiveMasterLogChunk(p);
                    case "MASTER_LOG_END" -> saveMasterLog();
                    default -> write("PROTO", "WARN", "알 수 없는 마스터 메시지: " + line);
                }
            }
            if (!terminatedByMaster && !closing) RunSession.current.error("Worker" + id + ": Master가 종료 신호 없이 연결을 닫았습니다.");
        } catch (IOException | RuntimeException e) {
            if (!closing && (!terminatedByMaster || e instanceof RuntimeException)) {
                RunSession.current.error("Worker" + id + " Master 수신 오류: " + e.getMessage());
            }
        } finally {
            synchronized (queueLock) {
                stopping = true;
                queueLock.notifyAll();
            }
            masterLogReceived.countDown();
        }
    }

    // Master 로그 조각 수신
    private void receiveMasterLogChunk(String[] p) {
        if (p.length < 2) throw new IllegalArgumentException("Master 로그 조각 누락");
        try {
            masterLogBytes.write(java.util.Base64.getDecoder().decode(p[1]));
        } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
    }

    // Master 로그를 Worker 실행 폴더에 저장
    private void saveMasterLog() {
        try {
            Files.write(RunSession.output("Master.txt"), masterLogBytes.toByteArray());
            masterLogSaved = true;
        } catch (IOException e) {
            RunSession.current.error("Master 로그 저장 실패: " + e.getMessage());
        } finally {
            masterLogReceived.countDown();
        }
    }

    // Worker1의 Master 로그 수신 대기
    private void waitForMasterLog() {
        if (id != 1) return;
        try {
            masterLogReceived.await(15, TimeUnit.SECONDS);
            if (!masterLogSaved) System.err.println("[확인 필요] Master 로그를 수신하지 못했습니다. 최종 결과는 부분 확인으로 표시합니다.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Master 확정 결과 시각 반영
    private void receiveResultAck(String[] p) {
        PendingResult result = pendingResults.remove(p[1]);
        if (result == null) return;
        updateMasterClock(Long.parseLong(p[3]));
        if (result.success) {
            write("PROC", "SUCCESS", "KV[" + result.task.id + "] 저장 완료, 처리="
                    + format(result.duration) + "초, 대기=" + format(result.wait) + "초");
        } else {
            write("PROC", "FAIL", "KV[" + result.task.id + "] 처리 실패, 20% 규칙");
        }
    }

    // Master 확정 P2P 시각 반영
    private void receiveP2pAck(String[] p) {
        updateMasterClock(Long.parseLong(p[5]));
        String action = "SENT".equals(p[1]) ? "이전 완료" : "수신 완료";
        write("LB", "SUCCESS", "워커" + p[2] + " " + action + ", KV=" + p[4]);
    }

    // Master 작업 Queue 삽입
    private void receiveTask(Task task) {
        updateMasterClock(task.enqueuedAt);
        boolean accepted;
        int size;
        synchronized (queueLock) {
            accepted = queue.size() < QUEUE_LIMIT;
            if (accepted) {
                if (task.retry) queue.addFirst(task);
                else queue.addLast(task);
                size = queue.size();
                received++;
                queueLock.notifyAll();
            } else {
                size = queue.size();
            }
        }
        if (!accepted) {
            pendingResults.put(task.id, new PendingResult(task, false, 0, 0));
            write("QUEUE", "FAIL", "KV[" + task.id + "] Queue 초과 거부 (10/10)");
            sendMaster("RESULT|" + task.id + "|FAIL|0.00|0.00|" + size);
            return;
        }
        String type = task.retry ? "우선 재시도" : "일반";
        write("RECV", "INFO", "KV[" + task.id + "] " + type + " 작업 수신, 대기열="
                + size + "/10");
        warnIfNeeded(size);
        sendMaster("STATUS|" + size);
        checkLoadBalance();
    }

    // Queue 작업 처리 반복
    private void processLoop() {
        while (true) {
            Task task = takeTask();
            if (task == null) return;
            long start = Math.max(workerAvailableAt, task.enqueuedAt);
            double wait = Math.max(0, (start - task.enqueuedAt) / 1000.0);
            double duration = ThreadLocalRandom.current().nextDouble(1.0, 3.0);
            workerAvailableAt = start + Math.round(duration * 1000.0);
            boolean ok = ThreadLocalRandom.current().nextInt(100) < 80;
            int size = queueSize();
            processed++;
            totalWait += wait;
            if (ok) success++;
            else fail++;
            pendingResults.put(task.id, new PendingResult(task, ok, duration, wait));
            sendMaster("RESULT|" + task.id + "|" + (ok ? "SUCCESS" : "FAIL") + "|"
                    + format(duration) + "|" + format(wait) + "|" + size);
            warnIfNeeded(size);
            sendMaster("STATUS|" + size);
            checkLoadBalance();
        }
    }

    // 다음 작업 대기·반환
    private Task takeTask() {
        synchronized (queueLock) {
            while (queue.isEmpty() && !stopping) {
                try {
                    queueLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return queue.pollFirst();
        }
    }

    // P2P 부하 분산 조건 확인
    private void checkLoadBalance() {
        long now = masterClockMillis;
        if (now < nextLoadCheck) return;
        nextLoadCheck = now + ThreadLocalRandom.current().nextLong(1_000, 3_001);
        List<Task> batch = new ArrayList<>();
        synchronized (queueLock) {
            if (queue.size() * 2 <= 15) return;
            for (int index = 0; index < 3 && !queue.isEmpty(); index++) batch.add(queue.pollLast());
        }
        int target = id % 4 + 1;
        if (transfer(target, batch)) {
            p2pSent += batch.size();
            sendMaster("P2P|SENT|" + target + "|" + batch.size() + "|" + taskIds(batch));
            int remaining = queueSize();
            warnIfNeeded(remaining);
            sendMaster("STATUS|" + remaining);
        } else {
            synchronized (queueLock) {
                for (int index = batch.size() - 1; index >= 0; index--) queue.addLast(batch.get(index));
                queueLock.notifyAll();
            }
        }
    }

    // 인접 Worker 작업 이전 요청
    private boolean transfer(int target, List<Task> batch) {
        if (batch.isEmpty()) return false;
        StringBuilder data = new StringBuilder();
        for (Task task : batch) {
            if (data.length() > 0) data.append(';');
            data.append(task.wire());
        }
        try (Socket peer = new Socket("127.0.0.1", 6000 + target);
             BufferedReader in = new BufferedReader(new InputStreamReader(peer.getInputStream()));
             PrintWriter out = new PrintWriter(peer.getOutputStream(), true)) {
            peer.setSoTimeout(5000);
            out.println("TRANSFER|" + id + "|" + data);
            return "ACK".equals(in.readLine());
        } catch (IOException e) {
            return false;
        }
    }

    // P2P 작업 수신 서버 시작
    private void startPeerListener() throws IOException {
        peerServer = new ServerSocket(peerPort);
        peerThread = new Thread(() -> {
            try (ServerSocket server = peerServer) {
                while (!stopping) {
                    try (Socket peer = server.accept();
                         BufferedReader in = new BufferedReader(new InputStreamReader(peer.getInputStream()));
                         PrintWriter out = new PrintWriter(peer.getOutputStream(), true)) {
                        peer.setSoTimeout(5000);
                        String line = in.readLine();
                        if (line == null) continue;
                        String[] p = line.split("\\|", 3);
                        if (!"TRANSFER".equals(p[0])) continue;
                        String[] encoded = p[2].split(";");
                        boolean accepted;
                        List<Task> tasks = new ArrayList<>();
                        for (String item : encoded) tasks.add(Task.fromWire(item));
                        synchronized (queueLock) {
                            accepted = queue.size() + tasks.size() <= QUEUE_LIMIT;
                            if (accepted) {
                                queue.addAll(tasks);
                                received += tasks.size();
                                queueLock.notifyAll();
                            }
                        }
                        if (accepted) {
                            p2pReceived += tasks.size();
                            sendMaster("P2P|RECEIVED|" + p[1] + "|" + tasks.size() + "|"
                                    + taskIds(tasks));
                            int size = queueSize();
                            warnIfNeeded(size);
                            sendMaster("STATUS|" + size);
                            out.println("ACK");
                        } else out.println("REJECT");
                    }
                }
            } catch (IOException | RuntimeException e) {
                if (!stopping) RunSession.current.error("Worker" + id + " P2P 수신 오류: " + e.getMessage());
            }
        }, "worker-peer-listener-" + id);
        peerThread.setDaemon(true);
        peerThread.start();
    }

    // 현재 Queue 크기 반환
    private int queueSize() {
        synchronized (queueLock) {
            return queue.size();
        }
    }

    // Queue 70% 초과 경고
    private void warnIfNeeded(int size) {
        if (size > 7) write("QUEUE", "WARN", "대기열 70% 초과, 크기=" + size + "/10");
    }

    // Master 메시지 전송
    private synchronized void sendMaster(String message) {
        if (masterOut != null) masterOut.println(message);
    }

    // Master 단일 가상 시각 반영
    private synchronized void updateMasterClock(long value) {
        masterClockMillis = Math.max(masterClockMillis, value);
    }

    // Worker 최종 통계 기록
    private void writeFinalStatistics() {
        write("STAT", "INFO", "=== WORKER" + id + " 최종 통계 ===");
        write("STAT", "INFO", "총 수신 작업: " + received);
        write("STAT", "SUCCESS", "성공: " + success);
        write("STAT", "FAIL", "실패: " + fail);
        write("STAT", "INFO", "평균 대기 시간: "
                + format(processed == 0 ? 0 : totalWait / processed) + "초");
        write("STAT", "INFO", "P2P 작업 전송/수신: " + p2pSent + " / " + p2pReceived);
        write("STAT", "INFO", "전체 수행 시간: " + VirtualClock.format(masterClockMillis) + "초");
        write("TERMINATE", "SUCCESS", "워커" + id + " 정상 연결 해제");
    }

    // Worker 로그 출력
    private void write(String event, String status, String message) {
        writeAt(masterClockMillis, event, status, message);
    }

    // 지정 Master 시각 로그 출력
    private void writeAt(long time, String event, String status, String message) {
        if (log != null) log.log(time, "WORKER" + id, event, status, message);
    }

    // P2P 작업 번호 문자열 생성
    private static String taskIds(List<Task> tasks) {
        StringBuilder ids = new StringBuilder();
        for (Task task : tasks) {
            if (ids.length() > 0) ids.append(',');
            ids.append("KV[").append(task.id).append(']');
        }
        return ids.toString();
    }

    // 분리 문자열 재결합
    private static String joinFrom(String[] values, int start) {
        StringBuilder result = new StringBuilder();
        for (int index = start; index < values.length; index++) {
            if (index > start) result.append('|');
            result.append(values[index]);
        }
        return result.toString();
    }

    // 소수점 둘째 자리 형식화
    private static String format(double value) {
        return String.format(java.util.Locale.US, "%.2f", value);
    }
}
