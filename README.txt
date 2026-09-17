분산 환경 Fault-Tolerant 키-값 저장소

1. 조원 정보

조원 1: 20223134 장승훈,
조원 2: 
조원 3: 

2. 프로그램 구성요소

src/DistributedKvApp.java: 실행 진입점, master 또는 workers 역할 선택
src/MasterNode.java: Master Node, Worker 연결 수락, KV 생성, 동적 배정, 재시도, 통계, 종료
src/WorkerNode.java: Worker Node, Worker Thread 4개, Ready Queue, 작업 처리, P2P 이전, Worker 로그
src/Shared.java: 공통 구성요소, Task, VirtualClock, EventLogger, WorkerInfo
AllDefinedLogs.txt: 로그 명세, 이벤트와 상태 코드 설명

3. 실행 환경

- 언어: Java 17
- Master 실행 환경: AWS EC2 Amazon Linux 2023
- Worker 실행 환경: Windows, macOS, Linux 로컬 PC
- Master Public IP: 32.236.94.251
- Master 포트: 기본 5000/TCP
- Worker P2P 포트: 6001~6004/TCP, 동일 로컬 PC의 127.0.0.1 사용

4. 컴파일 및 실행 방법

4-1. 컴파일

프로젝트 최상위 폴더에서 실행한다.

  javac -encoding UTF-8 -d out src/*.java
  jar cfe distributed-kv.jar DistributedKvApp -C out .

4-2. Master 실행

AWS EC2에서 Master가 실행 대기 상태이다. Master는 Worker 4개의 TCP 연결을 기다린 뒤 KV 5,000개 작업을 생성하고 배정을 시작한다. Master 실행이 끝나면 3초 뒤 자동으로 Master를 다시 시작시킨다.

4-3. Worker 실행

Master의 Public IP를 입력하여 로컬 PC에서 실행한다.

  java -jar distributed-kv.jar workers 32.236.94.251 5000

하나의 Worker 실행 명령은 Worker 1~4의 독립 Thread와 P2P 수신 포트 6001~6004를 생성한다.
Worker 실행이 끝나면 현재 로컬 폴더에 Worker1.txt~Worker4.txt가 생성된다. Master 주소가 원격 주소이면 Worker1은 종료 단계에서 TCP로 Master.txt를 수신하여 같은 로컬 폴더에 저장한다.

5. 동적 작업 분배 알고리즘

5-1. 알고리즘명: 최소 Ready Queue 우선 배정, 동률 순환 선택

1) Master는 재시도 우선 Queue에 작업이 있으면 일반 작업보다 먼저 선택한다.
2) 연결 상태이고 Queue 크기가 10 미만인 Worker만 후보로 선택한다.
3) 후보 중 Queue 크기가 가장 작은 Worker를 선택한다.
4) 동률이면 Worker ID 순환 순서로 선택한다.
5) Master는 TASK 메시지를 전송하고 해당 Worker의 예상 Queue 크기를 증가시킨다.
6) Worker의 STATUS, RESULT 메시지로 Queue 상태를 최신화한다.

장점

- 구현 단순성, Queue 길이 기반 작업 분산
- 과부하 Worker 배정 방지
- 재시도 작업의 우선 처리

단점

- 실제 CPU 성능, 네트워크 품질, 처리시간 차이 미반영
- Worker 상태 메시지 지연 시 일시적 Queue 상태 불일치 가능

6. P2P 부하 분산 알고리즘

6-1. 알고리즘명: 링(Ring) 토폴로지 기반 후단 작업 이전

Worker 연결 순서는 Worker1 -> Worker2 -> Worker3 -> Worker4 -> Worker1이다.
1) Worker는 가상 시간 기준 1~3초 랜덤 주기로 부하를 확인한다.
2) 예상 대기시간을 Queue 크기 x 평균 처리시간 2초로 계산한다.
3) 예상 대기시간이 15초를 초과하면 다음 Worker에 작업 이전을 요청한다.
4) 송신 Worker는 Queue 후단 작업 최대 3개를 TCP로 전송한다.
5) 수신 Worker가 Queue 여유 공간을 확인한 후 ACK 또는 REJECT를 전송한다.
6) ACK 수신 시 이전 완료, REJECT 또는 연결 실패 시 송신 Worker Queue 복구를 수행한다.

장점

- Master를 거치지 않는 직접 부하 분산
- 고정 링 구조에 따른 구현 단순성
- ACK 기반 이전 확인으로 작업 유실 방지

단점

- P2P 이전 시점에 따른 처리량 편차 발생 가능

7. 장애 처리 메커니즘

1) Worker는 각 작업을 80% 성공, 20% 실패 확률로 처리한다.
2) 실패 시 RESULT FAIL 메시지로 Master에 보고한다.
3) Master는 실패 작업을 Priority Retry Queue에 등록하고 재할당 횟수를 증가시킨다.
4) Master는 다음 배정 시 재시도 작업을 최우선으로 선택한다.
5) 재할당 작업도 동일한 80% 성공, 20% 실패 규칙을 적용한다.
6) 성공 결과가 수신될 때까지 위 과정을 반복한다.
7) Worker Queue 초과 거부도 FAIL 결과로 보고하여 Master 재할당 대상으로 처리한다.

8. 가상 시간, 로그, 종료

- 실제 Thread.sleep 미사용
- 가상 시각 권한: Master 단일 관리. Worker는 독립 Clock을 증가시키지 않고 Master 확정 시각을 수신하여 기록
- Worker 처리시간: 작업당 랜덤 1~3초를 RESULT 처리 시 Master 가상 시각에 반영
- 노드 간 통신: TASK 배정, RESULT 수신, P2P 이전 확인 시 Master 가상 시각에 1초 반영
- RESULT_ACK, P2P_ACK, TERMINATE 메시지: 시각 증가 없는 동기화 메시지. Worker 로그 시각 확정 용도
- Master 로그 위치: EC2 Master 실행 폴더의 Master.txt. 원격 Master 연결의 종료 단계에서 Worker1이 요청, 수신하여 Worker 로그가 저장되는 위치에 저장된다.
- Worker 로그 위치: 사용자가 workers 명령을 실행한 로컬 폴더의 Worker1.txt~Worker4.txt.
- 로그 형식: [clock] NODE | EVENT | STATUS | message
- Worker 로그: INIT, CONNECT, RECV, PROC, QUEUE, LB, 최종 통계와 전체 수행 시간 기록
- Master 로그: KV 생성, 배정, 결과, 재시도, P2P KV 번호, 총 성공·실패, Worker별 통계 기록
- 종료 조건: 고유 KV 5,000개 성공 처리 완료. 성공 완료 뒤의 중복 재시도 항목은 종료 시 폐기
- 종료 절차: Master TERMINATE 전송, Worker 최종 통계 및 TERMINATE_ACK 전송, Master 최종 통계 기록

9. 추가 구현 사항

- 고유 4자리 16진수 Key 생성, Value 범위 1~100
- Worker Ready Queue 최대 10개 제한
- Queue 크기 70% 초과 상태에서 Queue 변경 시 WARN 로그 기록
- Master와 Worker의 결과 통계 기록
- AllDefinedLogs.txt의 전체 로그 이벤트 명세 제공
