public class DistributedKvApp {
    // 실행 역할 분기
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("사용법: master <포트> | workers <Master_IP> <Master_포트> [--verbose]");
            return;
        }

        if ("master".equalsIgnoreCase(args[0])) {
            int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
            new MasterNode(port).run();
            return;
        }

        if ("workers".equalsIgnoreCase(args[0]) && args.length >= 3) {
            int port = Integer.parseInt(args[2]);
            if (port < 1 || port > 65535 || args.length > 4
                    || (args.length == 4 && !"--verbose".equals(args[3]))) {
                throw new IllegalArgumentException("workers <Master_IP> <1~65535> [--verbose]");
            }
            RunSession session = new RunSession(args[1], port, args.length == 4);
            RunSession.current = session;
            session.start();
            try {
                WorkerNode.startFour(args[1], port);
            } catch (Exception e) {
                session.error("Worker 실행 실패: " + e.getMessage());
            }
            System.exit(session.finish());
            return;
        }

        System.out.println("사용법: master <포트> | workers <Master_IP> <Master_포트> [--verbose]");
    }
}
