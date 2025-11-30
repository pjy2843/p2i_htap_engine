import asyncio
import boto3
import aiobotocore.session
import time
import os
from botocore.config import Config

# --- 설정 변수 ---
BUCKET_NAME = 'vldb-000'  # 🛑 본인의 S3 버킷 이름으로 변경하세요!
NUM_FILES = 80
FILE_SIZE_KB = 8
FILE_PREFIX = 'test-files/'  # 파일들을 저장할 S3 내의 경로
cfg = Config(max_pool_connections=80)


# ==============================================================================
# STEP 1: 테스트 파일 생성 및 S3 업로드
# ==============================================================================
# def setup_test_files():
#     """S3에 테스트용 더미 파일을 생성하고 업로드합니다."""
#     print(f"--- {NUM_FILES}개의 테스트 파일 생성을 시작합니다 ---")
#     s3_client = boto3.client('s3')
#     dummy_content = os.urandom(FILE_SIZE_KB * 1024)  # 8KB 크기의 랜덤 데이터 생성

#     for i in range(NUM_FILES):
#         file_key = f"{FILE_PREFIX}test-file-{i}.bin"
#         try:
#             s3_client.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=dummy_content)
#             print(f"  {i + 1}/{NUM_FILES} -> {file_key} 업로드 완료")
#         except Exception as e:
#             print(f"파일 업로드 중 오류 발생: {e}")
#             return []

#     print("--- 테스트 파일 생성 완료 ---\n")

#     # 업로드된 파일 목록 가져오기
#     try:
#         response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FILE_PREFIX)
#         object_keys = [obj['Key'] for obj in response.get('Contents', [])]
#         return object_keys
#     except Exception as e:
#         print(f"파일 목록 조회 중 오류 발생: {e}")
#         return []


# ==============================================================================
# STEP 2: 동기(Synchronous) 방식 테스트
# ==============================================================================
def run_sync_test(object_keys):
    """boto3를 사용하여 동기 방식으로 S3 파일들을 순차적으로 읽습니다."""
    print("--- 🐢 동기 방식 테스트 시작 ---")
    s3_client = boto3.client('s3')

    start_time = time.monotonic()

    for key in object_keys:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        # 실제 데이터를 읽는 동작을 포함하기 위해 read() 호출
        content = response['Body'].read()
        # print(f"  (Sync) Read {key}, size: {len(content)} bytes")

    end_time = time.monotonic()
    duration = end_time - start_time
    print(f"✅ 동기 방식 테스트 완료! 총 소요 시간: {duration:.4f} 초")
    return duration


# ---------------------------------------------------------------------
# STEP 3: 비동기(Asynchronous) 방식 테스트
# ---------------------------------------------------------------------
async def fetch_one(client, key):
    """단일 파일을 비동기적으로 읽는 코루틴"""
    response = await client.get_object(Bucket=BUCKET_NAME, Key=key)
    async with response["Body"] as stream:
        content = await stream.read()
        # print(f"(Async) Read {key}, size: {len(content)} bytes")


async def run_async_test_main(object_keys):
    """aiobotocore를 사용하여 비동기 방식으로 S3 파일들을 동시에 읽습니다."""
    print("\n--- ⚡️ 비동기 방식 테스트 시작 ---")
    start_time = time.monotonic()

    # 세션 생성
    session = aiobotocore.session.get_session()

    # region_name은 반드시 지정
    async with session.create_client("s3", region_name="ap-northeast-2", config=cfg) as client:
        # 80개의 파일 읽기 작업을 Task 리스트로 생성
        tasks = [fetch_one(client, key) for key in object_keys]

        # 모든 Task를 동시에 실행
        await asyncio.gather(*tasks)

    end_time = time.monotonic()
    duration = end_time - start_time
    print(f"✅ 비동기 방식 테스트 완료! 총 소요 시간: {duration:.4f} 초")
    return duration


# ==============================================================================
# 실험 실행 및 결과 비교
# ==============================================================================
if __name__ == "__main__":
    # 1. 테스트 파일 준비
    # keys_to_read = setup_test_files()
    s3_client = boto3.client('s3')

    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FILE_PREFIX)
    keys_to_read = [obj['Key'] for obj in response.get('Contents', [])]

    # 2. 동기 테스트 실행
    sync_duration = run_sync_test(keys_to_read)

    # 3. 비동기 테스트 실행
    async_duration = asyncio.run(run_async_test_main(keys_to_read))

    # 4. 결과 비교
    print("\n--- 📊 최종 결과 비교 ---")
    print(f"동기 방식   : {sync_duration:.4f} 초")
    print(f"비동기 방식 : {async_duration:.4f} 초")
    if async_duration > 0:
        speedup = sync_duration / async_duration
        print(f"\n결론: 비동기 방식이 동기 방식보다 약 {speedup:.2f}배 더 빨랐습니다.")