public class DistributedKvApp {
    // 실행 역할 분기
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("사용법: master <포트> | workers <Master_IP> <Master_포트>");
            return;
        }

        if ("master".equalsIgnoreCase(args[0])) {
            int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
            new MasterNode(port).run();
            return;
        }

        if ("workers".equalsIgnoreCase(args[0]) && args.length >= 3) {
            WorkerNode.startFour(args[1], Integer.parseInt(args[2]));
            return;
        }

        System.out.println("사용법: master <포트> | workers <Master_IP> <Master_포트>");
    }
}
