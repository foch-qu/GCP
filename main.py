import json
import base64
import logging
import os
from datetime import datetime
from flask import Flask, request, jsonify
import google.cloud.logging

app = Flask(__name__)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 初始化 Cloud Logging（可选）
try:
    client = google.cloud.logging.Client()
    client.setup_logging()
except Exception as e:
    logger.warning(f"Cloud Logging client failed to initialize: {e}")

@app.route('/healthz', methods=['GET'])
def health_check():
    """健康检查端点"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

@app.route('/', methods=['POST'])
def handle_nginx_log():
    """
    接收 Nginx 日志的主端点
    支持两种格式：
    1. 直接来自 Pub/Sub 的格式
    2. 直接来自 Sidecar 的格式
    """
    try:
        # 记录请求信息用于调试
        logger.info(f"Received request from {request.remote_addr}")
        
        content_type = request.headers.get('Content-Type', '')
        
        if 'application/json' in content_type:
            data = request.get_json()
            
            # 处理 Pub/Sub 格式
            if data and 'message' in data:
                return handle_pubsub_message(data)
            # 处理直接格式
            elif data and 'message' in data:
                return handle_direct_log(data)
            else:
                logger.warning(f"Unrecognized JSON format: {data}")
                return jsonify({"error": "Unrecognized JSON format"}), 400
                
        else:
            return jsonify({"error": "Unsupported Content-Type"}), 400
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

def handle_pubsub_message(data):
    """处理来自 Pub/Sub 的消息"""
    try:
        message = data['message']
        
        if 'data' in message:
            # 解码 base64 数据
            decoded_data = base64.b64decode(message['data']).decode('utf-8')
            log_entry = json.loads(decoded_data)
            
            # 处理日志条目
            process_log_entry(log_entry, source='pubsub')
            
            return jsonify({"status": "processed", "source": "pubsub"}), 200
        else:
            logger.warning("Pub/Sub message missing data field")
            return jsonify({"error": "Invalid Pub/Sub message"}), 400
            
    except Exception as e:
        logger.error(f"Error processing Pub/Sub message: {str(e)}")
        return jsonify({"error": "Failed to process Pub/Sub message"}), 500

def handle_direct_log(data):
    """处理直接发送的日志"""
    try:
        process_log_entry(data, source='direct')
        return jsonify({"status": "processed", "source": "direct"}), 200
    except Exception as e:
        logger.error(f"Error processing direct log: {str(e)}")
        return jsonify({"error": "Failed to process log"}), 500

def process_log_entry(log_entry, source='unknown'):
    """处理日志条目的核心逻辑"""
    try:
        # 提取日志信息
        timestamp = datetime.utcnow().isoformat()
        
        # 根据来源处理不同的格式
        if source == 'pubsub':
            # Cloud Logging 格式
            payload = log_entry.get('jsonPayload') or log_entry.get('textPayload', '')
            resource = log_entry.get('resource', {})
            labels = resource.get('labels', {})
            
            log_info = {
                'timestamp': timestamp,
                'source': source,
                'cluster': labels.get('cluster_name'),
                'namespace': labels.get('namespace_name'),
                'pod': labels.get('pod_name'),
                'container': labels.get('container_name'),
                'payload': payload,
                'raw_entry': log_entry
            }
            
        else:
            # 直接格式
            log_info = {
                'timestamp': timestamp,
                'source': source,
                'pod': log_entry.get('pod'),
                'namespace': log_entry.get('namespace'),
                'message': log_entry.get('message'),
                'raw_entry': log_entry
            }
        
        # 在这里添加你的业务逻辑：
        # 1. 存储到数据库
        # 2. 发送到分析服务
        # 3. 实时告警
        # 4. 数据转换
        
        # 示例：解析 Nginx 日志
        if 'message' in log_info:
            parsed_nginx = parse_nginx_log(log_info['message'])
            if parsed_nginx:
                log_info['parsed_nginx'] = parsed_nginx
        
        # 记录处理后的日志
        logger.info(f"Processed nginx log: {json.dumps(log_info, default=str)}")
        
        # 示例业务逻辑：检测错误状态码
        if log_info.get('parsed_nginx', {}).get('status', 0) >= 500:
            logger.error(f"Detected server error: {log_info}")
            
    except Exception as e:
        logger.error(f"Error in process_log_entry: {str(e)}")

def parse_nginx_log(log_line):
    """解析 Nginx 访问日志"""
    try:
        import re
        
        # 匹配 Nginx 默认日志格式
        # 127.0.0.1 - - [10/Oct/2023:10:30:45 +0000] "GET / HTTP/1.1" 200 612 "-" "Mozilla/5.0..."
        pattern = r'(\S+) - - \[(.*?)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "(.*?)" "(.*?)"'
        match = re.match(pattern, log_line)
        
        if match:
            return {
                'remote_addr': match.group(1),
                'time_local': match.group(2),
                'method': match.group(3),
                'path': match.group(4),
                'protocol': match.group(5),
                'status': int(match.group(6)),
                'body_bytes_sent': int(match.group(7)),
                'http_referer': match.group(8) if match.group(8) != "-" else "",
                'user_agent': match.group(9)
            }
        
        # 尝试解析 JSON 格式的 Nginx 日志
        if log_line.strip().startswith('{'):
            try:
                return json.loads(log_line)
            except:
                pass
                
    except Exception as e:
        logger.warning(f"Failed to parse nginx log: {e}")
    
    return None

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False)