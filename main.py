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

@app.route('/', methods=['GET', 'POST', 'OPTIONS'])
def handle_root():
    """处理根路径的所有请求"""
    if request.method == 'GET':
        return jsonify({
            "service": "Nginx Log Processor",
            "version": "1.0.0",
            "status": "running",
            "timestamp": datetime.utcnow().isoformat(),
            "endpoints": {
                "health_check": "/healthz (GET)",
                "log_receiver": "/ (POST)",
                "service_info": "/ (GET)"
            },
            "usage": {
                "GET": "Get service information",
                "POST": "Send nginx logs in JSON format"
            }
        })
    
    elif request.method == 'POST':
        return handle_nginx_log()
    
    elif request.method == 'OPTIONS':
        return '', 200

@app.route('/healthz', methods=['GET', 'OPTIONS'])
def health_check():
    """健康检查端点"""
    if request.method == 'OPTIONS':
        return '', 200
        
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "service": "nginx-log-processor"
    })

@app.route('/logs', methods=['POST', 'OPTIONS'])
def handle_logs():
    """专门的日志接收端点"""
    if request.method == 'OPTIONS':
        return '', 200
    return handle_nginx_log()

def handle_nginx_log():
    """处理 Nginx 日志 POST 请求"""
    try:
        logger.info(f"Received {request.method} request from {request.remote_addr}")
        
        # 检查内容类型
        content_type = request.headers.get('Content-Type', '')
        
        if not content_type or 'application/json' not in content_type:
            return jsonify({
                "error": "Unsupported Content-Type", 
                "required": "application/json",
                "received": content_type
            }), 400
        
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Empty JSON body"}), 400
            
        logger.info(f"Received data: {json.dumps(data, indent=2)}")
        
        # 处理 Pub/Sub 格式
        if 'message' in data and isinstance(data['message'], dict) and 'data' in data['message']:
            return handle_pubsub_format(data)
        # 处理直接日志格式
        elif 'message' in data:
            return handle_direct_format(data)
        else:
            # 尝试处理其他格式
            return handle_direct_format(data)
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

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
            "message": "Log processed successfully",
            "timestamp": datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing Pub/Sub message: {str(e)}")
        return jsonify({"error": f"Failed to process Pub/Sub message: {str(e)}"}), 500

def handle_direct_format(data):
    """处理直接发送的日志格式"""
    try:
        process_log_entry(data, 'direct')
        return jsonify({
            "status": "success",
            "source": "direct", 
            "message": "Log processed successfully",
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error processing direct log: {str(e)}")
        return jsonify({"error": f"Failed to process log: {str(e)}"}), 500

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
                'message': log_entry.get('message'),
                'raw_data': log_entry
            }
        
        # 解析 Nginx 日志（如果是文本格式）
        log_text = processed_log.get('message') or processed_log.get('payload')
        if isinstance(log_text, str):
            parsed_nginx = parse_nginx_log(log_text)
            if parsed_nginx:
                processed_log['parsed_nginx'] = parsed_nginx
        
        # 记录处理后的日志
        logger.info(f"Processed nginx log from {source}: {json.dumps(processed_log, default=str, indent=2)}")
        
        # 示例业务逻辑：检测错误状态码
        nginx_data = processed_log.get('parsed_nginx', {})
        if isinstance(nginx_data, dict):
            status = nginx_data.get('status', 0)
            if status >= 500:
                logger.error(f"🚨 Detected server error (5xx): Status {status}")
            elif status >= 400:
                logger.warning(f"⚠️ Detected client error (4xx): Status {status}")
            
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

# 添加 CORS 支持（如果需要）
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# 错误处理器
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found",
        "available_endpoints": {
            "/": ["GET", "POST"],
            "/healthz": ["GET"],
            "/logs": ["POST"]
        }
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "error": "Method not allowed for this endpoint",
        "requested_method": request.method,
        "allowed_methods": list(error.valid_methods) if hasattr(error, 'valid_methods') else ["GET", "POST"]
    }), 405

@app.errorhandler(500)
def internal_server_error(error):
    return jsonify({
        "error": "Internal server error",
        "message": str(error) if app.debug else "Something went wrong"
    }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Nginx Log Processor on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
