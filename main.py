import json
import base64
import logging
import os
import re
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# 配置结构化日志记录（Cloud Run 推荐）
class CloudLoggingFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "severity": record.levelname,
            "message": super().format(record),
            "logging.googleapis.com/sourceLocation": {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName
            }
        }
        return json.dumps(log_entry)

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 使用标准输出（Cloud Run 自动捕获）
handler = logging.StreamHandler()
handler.setFormatter(CloudLoggingFormatter())
logger.addHandler(handler)

@app.route('/', methods=['GET', 'POST', 'OPTIONS'])
def handle_root():
    """处理根路径的所有请求"""
    if request.method == 'GET':
        logger.info("Root endpoint accessed via GET")
        return jsonify({
            "service": "Nginx Log Processor",
            "version": "1.0.0",
            "status": "running",
            "timestamp": datetime.utcnow().isoformat(),
            "endpoints": {
                "health_check": "/healthz (GET)",
                "log_receiver": "/ (POST)",
                "service_info": "/ (GET)"
            }
        })
    
    elif request.method == 'POST':
        logger.info("Root endpoint accessed via POST")
        return handle_nginx_log()
    
    elif request.method == 'OPTIONS':
        return '', 200

@app.route('/healthz', methods=['GET', 'OPTIONS'])
def health_check():
    """健康检查端点"""
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("Health check endpoint accessed")
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
    logger.info("/logs endpoint accessed via POST")
    return handle_nginx_log()

def handle_nginx_log():
    """处理 Nginx 日志 POST 请求"""
    try:
        # 记录详细的请求信息
        logger.info("=== 开始处理 Nginx 日志请求 ===")
        logger.info(f"请求来源: {request.remote_addr}")
        logger.info(f"请求方法: {request.method}")
        logger.info(f"Content-Type: {request.headers.get('Content-Type')}")
        logger.info(f"Content-Length: {request.headers.get('Content-Length')}")
        logger.info(f"完整请求头: {dict(request.headers)}")
        
        # 获取原始数据
        raw_data = request.get_data(as_text=True)
        logger.info(f"原始请求数据: {raw_data}")
        
        if not raw_data or raw_data.strip() == '':
            logger.warning("收到空请求体")
            return jsonify({"error": "Empty JSON body"}), 400
        
        # 检查内容类型
        content_type = request.headers.get('Content-Type', '')
        if not content_type or 'application/json' not in content_type:
            logger.warning(f"不支持的 Content-Type: {content_type}")
            return jsonify({
                "error": "Unsupported Content-Type", 
                "required": "application/json",
                "received": content_type
            }), 400
        
        # 解析 JSON
        try:
            data = json.loads(raw_data)
            logger.info(f"成功解析 JSON 数据: {json.dumps(data, indent=2)}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析错误: {str(e)}")
            logger.error(f"有问题的数据: {raw_data}")
            return jsonify({
                "error": "Invalid JSON format",
                "details": str(e)
            }), 400
            
        # 根据数据格式路由处理
        if isinstance(data, dict) and 'message' in data and isinstance(data.get('message'), dict) and 'data' in data['message']:
            logger.info("检测到 Pub/Sub 格式数据")
            return handle_pubsub_format(data)
        else:
            logger.info("检测到直接日志格式数据")
            return handle_direct_format(data)
            
    except Exception as e:
        logger.error(f"处理请求时发生未预期错误: {str(e)}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

def handle_pubsub_format(data):
    """处理 Pub/Sub 格式的消息"""
    try:
        logger.info("开始处理 Pub/Sub 格式消息")
        message = data['message']
        base64_data = message['data']
        
        logger.info(f"Pub/Sub base64 数据: {base64_data}")
        
        # 解码 base64
        decoded_data = base64.b64decode(base64_data).decode('utf-8')
        logger.info(f"解码后的数据: {decoded_data}")
        
        # 解析 JSON
        log_entry = json.loads(decoded_data)
        logger.info(f"解析后的日志条目: {json.dumps(log_entry, indent=2)}")
        
        process_log_entry(log_entry, 'pubsub')
        
        logger.info("Pub/Sub 消息处理完成")
        return jsonify({
            "status": "success", 
            "source": "pubsub",
            "message": "Log processed successfully",
            "timestamp": datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"处理 Pub/Sub 消息时出错: {str(e)}", exc_info=True)
        return jsonify({"error": f"Failed to process Pub/Sub message: {str(e)}"}), 500

def handle_direct_format(data):
    """处理直接发送的日志格式"""
    try:
        logger.info("开始处理直接日志格式")
        logger.info(f"直接日志数据: {json.dumps(data, indent=2)}")
        
        if isinstance(data, list):
            logger.info(f"处理日志数组，共 {len(data)} 条记录")
            for i, item in enumerate(data):
                logger.info(f"处理第 {i+1} 条日志: {item}")
                process_log_entry(item, 'direct')
        else:
            process_log_entry(data, 'direct')
            
        logger.info("直接日志处理完成")
        return jsonify({
            "status": "success",
            "source": "direct", 
            "message": "Log processed successfully",
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"处理直接日志时出错: {str(e)}", exc_info=True)
        return jsonify({"error": f"Failed to process log: {str(e)}"}), 500

def process_log_entry(log_entry, source):
    """处理日志条目的核心逻辑"""
    try:
        logger.info(f"开始处理 {source} 类型的日志条目")
        
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
        
        logger.info(f"处理后的日志: {json.dumps(processed_log, indent=2, default=str)}")
        
        # 解析 Nginx 日志
        log_text = processed_log.get('message') or processed_log.get('payload')
        if isinstance(log_text, str):
            logger.info(f"尝试解析 Nginx 日志文本: {log_text}")
            parsed_nginx = parse_nginx_log(log_text)
            if parsed_nginx:
                processed_log['parsed_nginx'] = parsed_nginx
                logger.info(f"Nginx 日志解析结果: {json.dumps(parsed_nginx, indent=2)}")
        
        # 业务逻辑：检测错误状态码
        nginx_data = processed_log.get('parsed_nginx', {})
        if isinstance(nginx_data, dict):
            status = nginx_data.get('status', 0)
            if status >= 500:
                logger.error(f"🚨 检测到服务器错误 (5xx): 状态码 {status}")
            elif status >= 400:
                logger.warning(f"⚠️ 检测到客户端错误 (4xx): 状态码 {status}")
        
        logger.info(f"{source} 类型日志条目处理完成")
            
    except Exception as e:
        logger.error(f"处理日志条目时出错: {str(e)}", exc_info=True)

def parse_nginx_log(log_line):
    """解析 Nginx 访问日志"""
    try:
        if not isinstance(log_line, str):
            return None
            
        logger.info(f"解析 Nginx 日志: {log_line}")
        
        # 尝试解析 JSON 格式
        if log_line.strip().startswith('{'):
            try:
                result = json.loads(log_line)
                logger.info("成功解析为 JSON 格式")
                return result
            except:
                logger.warning("JSON 解析失败，尝试正则匹配")
                pass
        
        # 正则匹配 Nginx 默认格式
        pattern = r'(\S+) - - \[(.*?)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "(.*?)" "(.*?)"'
        match = re.match(pattern, log_line)
        
        if match:
            result = {
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
            logger.info("成功使用正则解析 Nginx 日志")
            return result
        else:
            logger.warning("无法解析的 Nginx 日志格式")
                
    except Exception as e:
        logger.error(f"解析 Nginx 日志时出错: {str(e)}")
    
    return None

# CORS 支持
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"启动 Nginx 日志处理器，端口: {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
