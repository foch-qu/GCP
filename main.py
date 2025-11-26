import json
import base64
import logging
import os
import re
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.route('/healthz', methods=['GET'])
def health_check():
    """健康检查端点 - 只允许 GET 方法"""
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "service": "nginx-log-processor"
    })

@app.route('/', methods=['GET', 'POST'])
def handle_requests():
    """
    处理根路径的请求
    GET: 返回服务信息
    POST: 处理 Nginx 日志
    """
    if request.method == 'GET':
        return jsonify({
            "service": "Nginx Log Processor",
            "version": "1.0.0",
            "endpoints": {
                "health_check": "/healthz (GET)",
                "log_receiver": "/ (POST)"
            },
            "usage": "Send POST requests with nginx logs in JSON format"
        })
    
    elif request.method == 'POST':
        return handle_nginx_log()

def handle_nginx_log():
    """处理 Nginx 日志 POST 请求"""
    try:
        logger.info(f"Received {request.method} request from {request.remote_addr}")
        
        content_type = request.headers.get('Content-Type', '')
        
        if 'application/json' in content_type:
            data = request.get_json()
            
            if not data:
                return jsonify({"error": "Empty JSON body"}), 400
                
            # 处理 Pub/Sub 格式
            if 'message' in data and isinstance(data['message'], dict) and 'data' in data['message']:
                return handle_pubsub_format(data)
            # 处理直接日志格式
            elif 'message' in data:
                return handle_direct_format(data)
            else:
                logger.warning(f"Unrecognized JSON format: {data}")
                return jsonify({"error": "Unrecognized JSON format"}), 400
        else:
            return jsonify({"error": "Unsupported Content-Type, must be application/json"}), 400
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

def handle_pubsub_format(data):
    """处理 Pub/Sub 格式的消息"""
    try:
        message = data['message']
        decoded_data = base64.b64decode(message['data']).decode('utf-8')
        log_entry = json.loads(decoded_data)
        
        process_log_entry(log_entry, 'pubsub')
        
        return jsonify({
            "status": "success", 
            "source": "pubsub",
            "message": "Log processed successfully"
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing Pub/Sub message: {str(e)}")
        return jsonify({"error": "Failed to process Pub/Sub message"}), 500

def handle_direct_format(data):
    """处理直接发送的日志格式"""
    try:
        process_log_entry(data, 'direct')
        return jsonify({
            "status": "success",
            "source": "direct", 
            "message": "Log processed successfully"
        }), 200
    except Exception as e:
        logger.error(f"Error processing direct log: {str(e)}")
        return jsonify({"error": "Failed to process log"}), 500

def process_log_entry(log_entry, source):
    """处理日志条目的核心逻辑"""
    try:
        timestamp = datetime.utcnow().isoformat()
        
        if source == 'pubsub':
            # Cloud Logging 格式
            payload = log_entry.get('jsonPayload') or log_entry.get('textPayload', '')
            resource = log_entry.get('resource', {})
            labels = resource.get('labels', {})
            
            processed_log = {
                'timestamp': timestamp,
                'source': source,
                'cluster': labels.get('cluster_name'),
                'namespace': labels.get('namespace_name'),
                'pod': labels.get('pod_name'),
                'container': labels.get('container_name'),
                'payload': payload
            }
            
        else:
            # 直接格式
            processed_log = {
                'timestamp': timestamp,
                'source': source,
                'pod': log_entry.get('pod'),
                'namespace': log_entry.get('namespace'),
                'message': log_entry.get('message')
            }
        
        # 解析 Nginx 日志（如果是文本格式）
        if isinstance(processed_log.get('message'), str):
            parsed_nginx = parse_nginx_log(processed_log['message'])
            if parsed_nginx:
                processed_log['parsed_nginx'] = parsed_nginx
                
        if isinstance(processed_log.get('payload'), str):
            parsed_nginx = parse_nginx_log(processed_log['payload'])
            if parsed_nginx:
                processed_log['parsed_nginx'] = parsed_nginx
        
        # 记录处理后的日志
        logger.info(f"Processed nginx log: {json.dumps(processed_log, default=str)}")
        
        # 示例业务逻辑：检测错误状态码
        nginx_data = processed_log.get('parsed_nginx', {})
        if isinstance(nginx_data, dict) and nginx_data.get('status', 0) >= 500:
            logger.error(f"Detected server error (5xx): {processed_log}")
        elif isinstance(nginx_data, dict) and nginx_data.get('status', 0) >= 400:
            logger.warning(f"Detected client error (4xx): {processed_log}")
            
    except Exception as e:
        logger.error(f"Error in process_log_entry: {str(e)}")

def parse_nginx_log(log_line):
    """解析 Nginx 访问日志"""
    try:
        if not isinstance(log_line, str):
            return None
            
        # 尝试解析 JSON 格式的 Nginx 日志
        if log_line.strip().startswith('{'):
            try:
                return json.loads(log_line)
            except:
                pass
        
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
                
    except Exception as e:
        logger.warning(f"Failed to parse nginx log: {e}")
    
    return None

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({"error": "Method not allowed for this endpoint"}), 405

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Nginx Log Processor on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
