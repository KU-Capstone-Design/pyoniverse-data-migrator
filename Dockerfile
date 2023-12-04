FROM python:3.10-slim-bullseye AS builder

WORKDIR /app
COPY . .
RUN apt-get update && apt-get upgrade && apt-get install locales git -y && locale-gen en_US.UTF-8 &&  \
    python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.10-slim-bullseye AS deployer

WORKDIR /app
# 해당 디렉토리에 있는 모든 하위항목들을 현재 디렉토리로 복사 ,, 여기서 지정
COPY --from=builder /app /app
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
ENTRYPOINT ["python", "app.py"]
CMD ["-h"]
