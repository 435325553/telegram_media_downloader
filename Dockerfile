# 基于官方镜像构建的二次开发版
# 官方镜像: tangyoha/telegram_media_downloader:latest
# 本镜像构建逻辑:
#   python 3.x slim 基础镜像 + 依赖安装 + 源码复制

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "media_downloader.py"]
