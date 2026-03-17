import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# 1. ตั้งค่าการเชื่อมต่อ (ใช้ Official Client)
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

topic_name = 'realtime_stock_data'
tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']

print(f"🚀 เริ่มจำลองการเทรดหุ้นและส่งเข้าท่อ: '{topic_name}' (กด Ctrl+C เพื่อหยุด)")

def delivery_report(err, msg):
    """ ฟังก์ชัน Callback ไว้เช็คว่าของส่งถึงปลายทางจริงๆ ไหม """
    if err is not None:
        print(f"❌ ส่งข้อมูลพลาด: {err}")
    else:
        print(f" {msg.value().decode('utf-8')}")

# 2. ลูปยิงข้อมูลแบบ Real-time
try:
    while True:
        stock_data = {
            'ticker': random.choice(tickers),
            'price': round(random.uniform(100.0, 500.0), 2),
            'event_time': datetime.utcnow().isoformat()
        }
        
        # แปลงข้อมูลเป็น JSON และยิงเข้า Kafka
        json_data = json.dumps(stock_data)
        producer.produce(topic_name, value=json_data.encode('utf-8'), callback=delivery_report)
        
        # สั่งให้ผลักข้อมูลออกจากคิว
        producer.poll(0)
        
        time.sleep(1) # ยิงวินาทีละ 1 ชุด
        
except KeyboardInterrupt:
    print("\n🛑 หยุดการส่งข้อมูล")
finally:
    # เคลียร์ข้อมูลที่อาจจะค้างอยู่ในท่อก่อนปิดโปรแกรม
    producer.flush()