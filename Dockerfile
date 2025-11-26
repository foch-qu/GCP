FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建非 root 用户（安全最佳实践）
RUN useradd -m -r appuser && chown -R appuser /app
USER appuser

# Cloud Run 会自动设置 $PORT 环境变量
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app