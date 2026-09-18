분산 환경 Fault-Tolerant 키-값 저장소

1. 조원 정보

조원 1: 20223134 장승훈 - Master, AWS, Master측 Network
조원 2: [학번 확인 필요] 정우진 - Worker Core, Queue, Processing / README 및 실행 스크립트 보완
조원 3: [학번 확인 필요] 김현중 - P2P, Logging/Stats, 통합, 시연 영상
※ 역할은 최초 분담 기준이며, 제출 전에 실제 수행 내역과 학번을 확인하여 확정한다.

2. 프로그램 구성요소

src/DistributedKvApp.java: 실행 진입점, master 또는 workers 역할 선택
src/MasterNode.java: Master Node, Worker 연결 수락, KV 생성, 동적 배정, 재시도, 통계, 종료
src/WorkerNode.java: Worker Node, Worker Thread 4개, Ready Queue, 작업 처리, P2P 이전, Worker 로그
src/Shared.java: 공통 구성요소, Task, VirtualClock, EventLogger, WorkerInfo
src/RunSession.java: 실행별 로그 폴더, 진행률, 상세 출력 전환, 최종 요약 및 검증 상태
AllDefinedLogs.txt: 로그 명세, 이벤트와 상태 코드 설명
run-workers.cmd: Windows Worker 실행 진입점 (더블클릭 또는 명령줄)
run-workers.ps1: Windows Java 환경 검사 및 Worker 실행 로직
run-workers.sh: macOS/Linux Java 환경 검사 및 Worker 실행

3. 실행 환경

- 언어: Java 17
- Master 실행 환경: AWS EC2 Amazon Linux 2023
- Worker 실행 환경: Windows, macOS, Linux 로컬 PC
- Master Public IP: 32.236.94.251
- Master 포트: 기본 5000/TCP
- Worker P2P 포트: 6001~6004/TCP, 동일 로컬 PC의 127.0.0.1 사용

4. 사전 작업

4-1. 공통 준비사항

- 개발 기준은 JDK 17이다. JDK에는 java(실행), javac(컴파일), jar(JAR 생성)가 포함된다.
- 아래 빌드 명령은 --release 17을 사용한다. 실행 스크립트는 Java 17 이상을 허용한다.
- JAR 실행만 할 때는 javac가 필요하지 않지만, 소스 컴파일을 위해서는 JDK가 필요하다.
- JAVA_HOME은 JDK 설치 폴더, PATH는 실행 파일을 찾을 폴더 목록이다.
  JAVA_HOME에는 bin을 제외한 경로를, PATH에는 JDK의 bin 경로를 추가한다.
- 기존 PATH 전체를 지우거나 덮어쓰지 않는다. 설치 후 터미널을 새로 연다.

4-2. Windows

1) https://adoptium.net/temurin/releases/?version=17 에서 Windows용 JDK 17 MSI를 설치한다.
   PC 아키텍처에 맞는 파일을 선택한다. 설치 옵션에서 PATH 추가 및 JAVA_HOME 설정을 선택할 수 있다.
2) 자동 설정되지 않았다면 Windows 검색에서 '시스템 환경 변수 편집' -> '환경 변수'를 연다.
   사용자 변수 JAVA_HOME을 만들고 실제 JDK 설치 폴더를 입력한다.
   예: C:\Program Files\Eclipse Adoptium\<설치된 JDK 17 폴더명>
   사용자 변수 Path에 %JAVA_HOME%\bin 항목을 추가한다.
   꺾쇠 안의 폴더명은 실제 설치 폴더명으로 바꾸며, 경로 값에 따옴표를 넣지 않는다.
3) 새 PowerShell 또는 명령 프롬프트에서 확인한다.

   java -version
   javac -version
   jar --version

   JDK 17을 설치했다면 모두 17로 시작해야 한다. 다른 버전이 선택되면 다음 명령으로 경로를 확인한다.

   where.exe java
   where.exe javac

4-3. macOS (기본 zsh 기준)

1) 위 Temurin 다운로드 페이지에서 macOS용 JDK 17 PKG를 설치한다.
   Apple Silicon은 aarch64, Intel Mac은 x64를 선택한다.
2) ~/.zshrc에 아래 내용을 추가한다. 기존 Java 설정이 있다면 중복 추가 대신 수정한다.

   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   export PATH="$JAVA_HOME/bin:$PATH"

3) 설정을 적용하고 확인한다.

   source ~/.zshrc
   java -version
   javac -version
   jar --version

4-4. Linux

배포판에 맞는 설치 명령을 사용한다.

Ubuntu 22.04/24.04:
   sudo apt update
   sudo apt install openjdk-17-jdk

Amazon Linux 2023 (AWS Master):
   sudo yum install java-17-amazon-corretto-devel

설치 후 아래 명령으로 javac 버전과 실제 경로를 확인한다.

   javac -version
   readlink -f "$(command -v javac)"

javac가 17 버전인지 확인한다. 다른 버전이면 배포판의 alternatives 설정에서 JDK 17을 선택하거나
설치된 JDK 17 폴더를 직접 확인한다. 출력 경로의 마지막 /bin/javac를 제외한 부분이 JDK 폴더이다.
Bash 사용자는 ~/.bashrc에 다음을 추가한다. <JDK 17 설치 경로>는 실제 경로로 바꾼다.

   export JAVA_HOME="<JDK 17 설치 경로>"
   export PATH="$JAVA_HOME/bin:$PATH"

   source ~/.bashrc
   java -version
   javac -version
   jar --version

zsh 사용자는 ~/.zshrc에 설정하고 source ~/.zshrc로 적용한다.
macOS/Linux에서 실행 경로를 확인하려면 command -v java 및 command -v javac를 사용한다.

4-5. 검사 및 문제 해결

- java를 찾을 수 없음: 미설치 또는 PATH 설정 문제일 수 있다. JDK 설치 위치와 환경변수를 확인한다.
- java만 되고 javac가 안 됨: JDK 설치 여부와 JDK의 bin 경로를 확인한다.
- JAVA_HOME만 정상인 경우에도 실행 스크립트는 해당 폴더의 Java를 직접 사용할 수 있다.
- 스크립트는 JAVA_HOME, PATH 순서로 실행 가능한 Java 17 이상을 찾는다.
  찾지 못했다고 미설치로 단정하지 않으며, 임의의 설치 폴더 전체를 검색하지 않는다.
- 자동 설치, 시스템 환경변수 변경은 하지 않는다. Windows CMD는 자식 PowerShell 프로세스에만
  ExecutionPolicy Bypass를 적용하며 저장된 실행 정책은 변경하지 않는다. 조직의 그룹 정책은 우회하지 않는다.
- 환경만 검사하고 네트워크 연결이나 Worker 로그 생성을 하지 않으려면:

   Windows PowerShell: .\run-workers.cmd -CheckOnly
   macOS/Linux: bash run-workers.sh --check

설치 참고 문서:
   https://adoptium.net/installation
   https://docs.oracle.com/en/java/javase/17/install/installation-guide.pdf
   https://docs.aws.amazon.com/corretto/latest/corretto-17-ug/amazon-linux-install.html

5. 컴파일 및 실행 방법

5-1. 컴파일

프로젝트 최상위 폴더에서 실행한다.

  javac --release 17 -encoding UTF-8 -d out src/DistributedKvApp.java src/MasterNode.java src/WorkerNode.java src/Shared.java src/RunSession.java
  jar cfe distributed-kv.jar DistributedKvApp -C out .

5-2. Master 실행

AWS EC2의 프로젝트 폴더에서 다음 명령으로 Master를 실행한다.

  java -jar distributed-kv.jar master 5000

Master는 KV 5,000개 작업을 먼저 생성하고 Worker 4개의 TCP 연결이 완료되면 배정을 시작한다.
Worker 실행 전에 Master가 실행 중인지와 AWS 보안 그룹의 TCP 5000 접근 허용 여부를 확인한다.
자동 재시작은 Java 소스에 포함되지 않으므로, AWS에서 별도로 설정했는지는 서버 담당자에게 확인한다.

5-3. Worker 실행

실행 스크립트 사용 (권장):

  Windows PowerShell: .\run-workers.cmd 32.236.94.251 5000
  macOS/Linux: bash run-workers.sh 32.236.94.251 5000

Windows에서 run-workers.cmd를 더블클릭하면 Java 검사 후 Master IP와 포트를 입력받는다.
포트 입력에서 Enter를 누르면 5000을 사용한다. 종료 후 메시지를 읽을 수 있도록 키 입력을 기다린다.
macOS/Linux에서도 인자 없이 bash run-workers.sh를 실행하면 IP와 포트를 입력받는다.
명령줄에서 IP만 전달하면 포트는 5000이다. IP는 현재 Master 주소에 맞게 변경한다.
스크립트는 자신이 있는 프로젝트 폴더에서 JAR를 실행한다.
매 실행마다 runs/<시작시각>-<UUID>/ 폴더를 만들며 이전 실행 로그는 덮어쓰지 않는다.
Windows의 run-workers.cmd와 run-workers.ps1은 같은 폴더에 있어야 한다.

기존 명령어로 직접 실행:

Master의 Public IP를 입력하여 로컬 PC의 프로젝트 폴더에서 실행한다.

  java -jar distributed-kv.jar workers 32.236.94.251 5000

직접 실행하면 스크립트의 사전 검사를 거치지 않는다. java가 PATH에서 인식되어야 하며,
그렇지 않으면 Java 실행 파일의 전체 경로를 지정한다.

하나의 Worker 실행 명령은 Worker 1~4의 독립 Thread와 P2P 수신 포트 6001~6004를 생성한다.
직접 실행해도 현재 작업 폴더 아래 runs/<시작시각>-<UUID>/에 Worker1.txt~Worker4.txt가 기록된다.
Worker1은 종료 단계에서 TCP로 Master.txt를 요청하여 같은 실행 폴더에 저장한다 (로컬 Master 포함).

5-4. 화면 표시 및 실행 결과

기본 화면은 연결 수, 고유 작업 성공 수, 실패 보고 수, 가상 시각을 최대 0.5초마다 표시한다.
이 주기는 화면 갱신용 실제 시간이며 작업 처리나 가상 시계에는 영향을 주지 않는다.
모든 상세 이벤트는 화면 표시 여부와 무관하게 Worker 로그 파일에 기록된다.
실행 중 v를 입력하고 Enter를 누르면 상세 출력을 켜거나 끌 수 있다 (입력이 가능한 터미널에서 사용).
처음부터 상세 출력을 켜려면:

  Windows PowerShell: .\run-workers.cmd 32.236.94.251 5000 -Detailed
  macOS/Linux: bash run-workers.sh 32.236.94.251 5000 --verbose
  직접 실행: java -jar distributed-kv.jar workers 32.236.94.251 5000 --verbose

모든 Worker 종료 및 Master 로그 수신 대기 후 통합 요약을 한 번 출력한다.
실제 경과 시간과 가상 수행 시간, Worker별 통계, Master 로그 검증 상태를 구분한다.
P2P 0회는 이번 실행에서 확인되지 않았다는 안내이며 자동으로 오류 판정하지 않는다.
summary.txt에 화면의 최종 요약을, run.json에 실행 ID/서버/시각/상태를 저장한다.

  COMPLETED: Worker 및 Master의 고유 성공 5,000개와 정상 종료 기록 확인 (종료 코드 0)
  PARTIAL: Worker 완료 확인, Master 로그 미수신 또는 검증 불일치 (종료 코드 2)
  FAILED: 실행 오류 또는 Worker 완료 조건 미충족 (종료 코드 1)
  RUNNING: 실행 중. 강제 종료로 상태 갱신이 불가능했다면 남아 있을 수 있음
  ABORTED: 종료 훅에서 중단을 감지함. 작성된 로그는 보존됨

완료 판정은 로그 기준이며 모든 key/value의 메모리 내용이나 모든 장애 상황을 검증하는 것은 아니다.
연결 제한 시간은 10초, Master 수신 무응답 제한은 30초, 최종 Master 로그 대기는 최대 15초이다.
제출할 때 검증한 실행 폴더의 Master.txt 및 Worker1.txt~Worker4.txt를 선택한다.
실행 폴더는 자동 삭제하지 않으며 Git에서는 runs/를 제외한다.

동시 실행 정책은 아직 변경하지 않았다. 폴더가 분리되어도 같은 PC의 P2P 포트 6001~6004와
공용 Master의 Worker ID 1~4는 공유된다. 팀 합의 전에는 같은 Master에 대한 실행을 겹치지 않게 한다.
AWS Master 자체의 Master.txt는 기존 서버 동작대로 덮어쓰므로 서버 로그 보존은 별도 논의가 필요하다.

로컬 통합 검증 (Python 3 필요, AWS에 연결하지 않음):
  python tests/verify_console.py
  기본/상세 출력, 실행 중 전환, 순차 실행 로그 보존, Master 로그 누락, 연결 실패를 확인한다.

6. 동적 작업 분배 알고리즘

6-1. 알고리즘명: 최소 Ready Queue 우선 배정, 동률 순환 선택

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

7. P2P 부하 분산 알고리즘

7-1. 알고리즘명: 링(Ring) 토폴로지 기반 후단 작업 이전

Worker 연결 순서는 Worker1 -> Worker2 -> Worker3 -> Worker4 -> Worker1이다.
1) Worker는 작업 수신·처리 시 부하 확인 함수를 호출하며, Master에서 받은 가상 시각이 다음 확인 시각에 도달했을 때 검사한다. 다음 확인 시각은 1~3초 뒤로 정한다.
2) 예상 대기시간을 Queue 크기 x 평균 처리시간 2초로 계산한다.
3) 예상 대기시간이 15초를 초과하면 다음 Worker에 작업 이전을 요청한다.
4) 송신 Worker는 Queue 후단 작업 최대 3개를 TCP로 전송한다.
5) 수신 Worker가 Queue 여유 공간을 확인한 후 ACK 또는 REJECT를 전송한다.
6) ACK 수신 시 이전 완료, REJECT 또는 연결 실패 시 송신 Worker Queue 복구를 수행한다.

장점

- Master를 거치지 않는 직접 부하 분산
- 고정 링 구조에 따른 구현 단순성
- ACK 기반으로 이전 결과를 확인하고, 전송 실패 시 Queue 복구 시도

단점

- P2P 이전 시점에 따른 처리량 편차 발생 가능
- P2P 발생 횟수는 실행마다 달라진다. 최종 요약 및 Master 로그에서 이번 실행의 횟수를 확인한다.

8. 장애 처리 메커니즘

1) Worker는 각 작업을 80% 성공, 20% 실패 확률로 처리한다.
2) 실패 시 RESULT FAIL 메시지로 Master에 보고한다.
3) Master는 실패 작업을 Priority Retry Queue에 등록하고 재할당 횟수를 증가시킨다.
4) Master는 다음 배정 시 재시도 작업을 최우선으로 선택한다.
5) 재할당 작업도 동일한 80% 성공, 20% 실패 규칙을 적용한다.
6) 성공 결과가 수신될 때까지 위 과정을 반복한다.
7) Worker Queue 초과 거부도 FAIL 결과로 보고하여 Master 재할당 대상으로 처리한다.

9. 가상 시간, 로그, 종료

- 실제 Thread.sleep 미사용
- 가상 시각 권한: Master 단일 관리. Worker는 독립 Clock을 증가시키지 않고 Master 확정 시각을 수신하여 기록
- Worker 처리시간: 작업당 랜덤 1~3초를 RESULT 처리 시 Master 가상 시각에 반영
- 노드 간 통신: TASK 배정, RESULT 수신, P2P 이전 확인 시 Master 가상 시각에 1초 반영
- RESULT_ACK, P2P_ACK, TERMINATE 메시지: 시각 증가 없는 동기화 메시지. Worker 로그 시각 확정 용도
- Master 로그 위치: EC2 Master 실행 폴더의 Master.txt. 종료 단계에서 Worker1이 요청하여 로컬 실행별 폴더에 복사본을 저장한다.
- Worker 로그 위치: runs/<시작시각>-<UUID>/Worker1.txt~Worker4.txt.
- 로그 형식: [clock] NODE | EVENT | STATUS | message
- Worker 로그: INIT, CONNECT, RECV, PROC, QUEUE, LB, 최종 통계와 전체 수행 시간 기록
- Master 로그: KV 생성, 배정, 결과, 재시도, P2P KV 번호, 총 성공·실패, Worker별 통계 기록
- 현재 코드의 종료 판단: SUCCESS 수신 누적 횟수 5,000회. 고유 저장 건수는 별도로 최종 통계에 출력하며, 중복 SUCCESS를 제외하는 검증은 추가 확인이 필요하다.
- 종료 절차: Master TERMINATE 전송, Worker 처리 루프 종료 및 TERMINATE_ACK 전송, 각 노드 최종 통계 기록

10. 추가 구현 사항

- 고유 4자리 16진수 Key 생성, Value 범위 1~100
- Worker Ready Queue 최대 10개 제한
- Queue 크기 70% 초과 상태에서 Queue 변경 시 WARN 로그 기록
- Master와 Worker의 결과 통계 기록
- AllDefinedLogs.txt의 전체 로그 이벤트 명세 제공
