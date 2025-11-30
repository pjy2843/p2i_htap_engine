#Dockerfile

FROM ubuntu:22.04

# 비대화 모드 설정: 이거 따로 설정안하면 지역선택하라고 나오는ㄷ 
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul

# 1. 시스템 업데이트 및 필수 패키지 모두 설치
# 여기서 curl, wget, git, 빌드 도구 등을 모두 설치합니다.
RUN apt update && apt install -y \
    git build-essential cmake ninja-build \
    libssl-dev liburing-dev curl vim python3 python3-pip curl zip unzip awscli tmux wget vim lld libjemalloc-dev \
    openssl g++ libcurlpp-dev zlib1g-dev libcurl4-openssl-dev

CMD ["/bin/bash"]