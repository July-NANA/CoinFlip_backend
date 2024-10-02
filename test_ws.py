import websocket
import json
import threading
import time

def on_message(ws, message):
    try:
        data = json.loads(message)
        print(json.dumps(data, indent=2))
    except json.JSONDecodeError:
        print("接收到的消息不是有效的JSON格式")

def on_error(ws, error):
    print(f"发生错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### 连接关闭 ###")
    print(f"关闭状态码: {close_status_code}, 信息: {close_msg}")

def on_open(ws):
    print("### 连接建立 ###")

def run_websocket():
    ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr"
    proxy_host = "127.0.0.1"
    proxy_port = 7890  # 例如 8080
    proxy_type = "http"  # 或者 "https"

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(
                ping_interval=60,
                ping_timeout=10,
                http_proxy_host=proxy_host,
                http_proxy_port=proxy_port,
                proxy_type=proxy_type
            )
        except Exception as e:
            print(f"连接异常: {e}")
        print("等待5秒后尝试重新连接...")
        time.sleep(5)

if __name__ == "__main__":
    ws_thread = threading.Thread(target=run_websocket)
    ws_thread.daemon = True
    ws_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序终止")