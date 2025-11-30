#include "cloud/provider.hpp"
#include "network/tasked_send_receiver.hpp"
#include "network/transaction.hpp"
#include "network/config.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <chrono>

using namespace std;

int main(int /*argc*/, char** /*argv*/) {
    // ============ ⬇️ 디버깅 코드 추가 ⬇️ ============
    std::cerr << "\n\n!!!!!!!!!! MAIN FUNCTION STARTED !!!!!!!!!!\n\n" << std::endl;
    // ===========================================

    // ▼▼▼▼▼▼▼▼▼▼▼▼▼ 1. 환경 변수에서 직접 Credential 읽어오기 ▼▼▼▼▼▼▼▼▼▼▼▼▼
    const char* keyIdEnv = getenv("AWS_ACCESS_KEY_ID");
    const char* secretEnv = getenv("AWS_SECRET_ACCESS_KEY");

    if (!keyIdEnv || !secretEnv) {
        cerr << "Error: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set." << endl;
        return 1;
    }
    string keyId(keyIdEnv);
    string secret(secretEnv);
    // ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

    
    // ---------------------------------------------------------------------
    // 1. 설정: 본인의 S3 버킷 정보와 테스트 파일 정보로 변경하세요.
    // ---------------------------------------------------------------------
    string bucket_uri = "s3://vldb-000:ap-northeast-2"; // 🛑 "s3://버킷이름:리전" 형식으로 수정
    string file_prefix = "test-files/";
    const int num_files = 80;

    // 테스트할 80개 파일의 전체 경로 생성
    vector<string> file_keys;
    for (int i = 0; i < num_files; ++i) {
        file_keys.push_back(file_prefix + "test-file-" + to_string(i) + ".bin");
    }
    

    


    // ---------------------------------------------------------------------
    // 2. AnyBlob 초기화
    // ---------------------------------------------------------------------
    anyblob::network::TaskedSendReceiverGroup group;
    std::cerr << "DEBUG: 1. TaskedSendReceiverGroup created.\n" << std::endl;
    
    auto sendReceiverHandle = group.getHandle();
    std::cerr << "DEBUG: 2. Handle created.\n" << std::endl;

    

    bool https = true; // 실제 AWS S3 사용 시 반드시 true로 설정
    // ▼▼▼▼▼▼▼▼▼▼▼▼▼ 2. 읽어온 Credential을 명시적으로 전달 ▼▼▼▼▼▼▼▼▼▼▼▼▼
    auto provider = anyblob::cloud::Provider::makeProvider(bucket_uri, https, keyId, secret, &sendReceiverHandle);
    // ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    std::cerr << "DEBUG: 3. Provider created.\n" << std::endl;



    // 1. 처리량(Throughput)은 라이브러리 기본값(8000 Mbit/s)을 사용합니다.
    const uint64_t throughput = anyblob::network::Config::defaultCoreThroughput;
    
    // 2. 동시성(Concurrency)은 우리가 원하는 값(80)으로 설정합니다.
    const unsigned concurrency = 20;
    
    // 3. 네트워크 대역폭(network)은 일단 처리량과 같은 값으로 가정합니다.
    const uint64_t network_bandwidth = throughput;

    anyblob::network::Config tuned_config{throughput, concurrency, network_bandwidth};
    group.setConfig(tuned_config);


    // auto config = provider->getConfig(sendReceiverHandle);
    // std::cerr << "DEBUG: 4. Config retrieved.\n" << std::endl;

    // group.setConfig(config);
    // std::cerr << "DEBUG: 5. Group configured.\n" << std::endl;

    cout << "--- 🚀 AnyBlob (io_uring) 테스트 시작 ---" << endl;
    cout << num_files << "개의 파일을 비동기적으로 다운로드합니다..." << endl;

    // ---------------------------------------------------------------------
    // 3. 시간 측정 시작 및 요청 생성
    // ---------------------------------------------------------------------
    auto start = chrono::high_resolution_clock::now();

    anyblob::network::Transaction getTxn(provider.get());
    for (const auto& key : file_keys) {
        getTxn.getObjectRequest(key);
    }
    
    // ---------------------------------------------------------------------
    // 4. 모든 요청을 동기적으로 처리 완료될 때까지 대기
    // ---------------------------------------------------------------------
    getTxn.processSync(sendReceiverHandle);
    
    // ---------------------------------------------------------------------
    // 5. 시간 측정 종료 및 결과 분석
    // ---------------------------------------------------------------------
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;
    
    int success_count = 0;
    for (const auto& it : getTxn) {
        if (it.success()) {
            success_count++;
        }
    }

    cout << "\n--- 📊 테스트 결과 ---" << endl;
    cout << "총 소요 시간: " << elapsed.count() << " 초" << endl;
    cout << "성공한 파일 수: " << success_count << " / " << num_files << endl;

    if (success_count != num_files) {
        cout << "⚠️ 일부 요청이 실패했습니다." << endl;
        // 실패한 요청에 대한 상세 정보 출력 (필요 시 주석 해제)
        /*
        for (size_t i = 0; i < getTxn.size(); ++i) {
            if (!getTxn[i].success()) {
                cout << "  - 실패한 파일: " << file_keys[i] << endl;
            }
        }
        */
    }

    return 0;
}