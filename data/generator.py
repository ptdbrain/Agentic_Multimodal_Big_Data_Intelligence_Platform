import os
import json
import random
import datetime
from pathlib import Path
from typing import List, Dict, Any

CATEGORIES = {
    "Smartphone": ["Apple", "Samsung", "Xiaomi", "Google", "Asus"],
    "Laptop": ["Apple", "Dell", "Lenovo", "Asus"],
    "Tablet": ["Apple", "Samsung", "Xiaomi"],
    "Accessories": ["Apple", "Sony", "Samsung"]
}

PRODUCTS_METADATA = [
    {"id": "iphone_15_pro", "name": "iPhone 15 Pro 128GB", "brand": "Apple", "category": "Smartphone", "base_price": 27990000},
    {"id": "iphone_15", "name": "iPhone 15 128GB", "brand": "Apple", "category": "Smartphone", "base_price": 19490000},
    {"id": "galaxy_s24_ultra", "name": "Samsung Galaxy S24 Ultra 256GB", "brand": "Samsung", "category": "Smartphone", "base_price": 29990000},
    {"id": "galaxy_s24", "name": "Samsung Galaxy S24 128GB", "brand": "Samsung", "category": "Smartphone", "base_price": 17990000},
    {"id": "xiaomi_14_pro", "name": "Xiaomi 14 Pro 256GB", "brand": "Xiaomi", "category": "Smartphone", "base_price": 16990000},
    {"id": "pixel_8_pro", "name": "Google Pixel 8 Pro 128GB", "brand": "Google", "category": "Smartphone", "base_price": 18500000},
    {"id": "rog_phone_8", "name": "Asus ROG Phone 8 Pro 512GB", "brand": "Asus", "category": "Smartphone", "base_price": 24990000},
    {"id": "macbook_pro_m3", "name": "MacBook Pro 14 M3 512GB", "brand": "Apple", "category": "Laptop", "base_price": 39990000},
    {"id": "macbook_air_m3", "name": "MacBook Air 13 M3 256GB", "brand": "Apple", "category": "Laptop", "base_price": 27490000},
    {"id": "dell_xps_15", "name": "Dell XPS 15 9530 Core i7", "brand": "Dell", "category": "Laptop", "base_price": 45990000},
    {"id": "thinkpad_x1_carbon", "name": "Lenovo ThinkPad X1 Carbon Gen 11", "brand": "Lenovo", "category": "Laptop", "base_price": 38990000},
    {"id": "ipad_pro_m4", "name": "iPad Pro 11 M4 OLED 256GB", "brand": "Apple", "category": "Tablet", "base_price": 28990000},
    {"id": "galaxy_tab_s9", "name": "Samsung Galaxy Tab S9 128GB", "brand": "Samsung", "category": "Tablet", "base_price": 16490000},
    {"id": "sony_wh1000xm5", "name": "Sony WH-1000XM5 Wireless Headphones", "brand": "Sony", "category": "Accessories", "base_price": 7990000},
    {"id": "airpods_pro_2", "name": "Apple AirPods Pro 2 USB-C", "brand": "Apple", "category": "Accessories", "base_price": 5490000}
]

POSITIVE_FEEDBACK_VI = [
    "Sản phẩm dùng rất mượt, pin trâu hơn mong đợi.",
    "Chất lượng hoàn thiện tuyệt vời, màn hình rất đẹp và sáng.",
    "Giao hàng nhanh, đóng gói cẩn thận, dùng 2 tuần chưa thấy lỗi gì.",
    "Camera chụp đêm rất ấn tượng, độ chi tiết cao.",
    "Rất hài lòng với quyết định nâng cấp lần này, 10/10."
]

NEGATIVE_FEEDBACK_VI = [
    "Máy nhanh nóng khi dùng 4G hoặc quay video 4K.",
    "Pin tụt khá nhanh, chỉ được nửa ngày là phải cắm sạc.",
    "Sau cập nhật phần mềm xuất hiện sọc màn hình nhẹ.",
    "Giá hơi đắt so với trải nghiệm thực tế mang lại.",
    "Thiết kế dễ bám vân tay, loa ngoài hơi rè ở âm lượng lớn."
]

POSITIVE_FEEDBACK_EN = [
    "Excellent device, blazing fast performance and gorgeous display.",
    "Battery life easily lasts through a full heavy-use day.",
    "Build quality is superb, feels very premium in hand.",
    "Cameras are outstanding in low light conditions.",
    "Highly recommended! Worth every single penny."
]

NEGATIVE_FEEDBACK_EN = [
    "Device gets surprisingly warm during intensive multitasking.",
    "Battery drainage issue observed after recent firmware update.",
    "Customer service was unresponsive when reporting cosmetic defect.",
    "Overpriced for the features offered compared to competitors.",
    "Occasional software glitch causing random restarts."
]

def generate_products() -> List[Dict[str, Any]]:
    products = []
    now = datetime.datetime(2026, 9, 24, 8, 0, 0)
    for p in PRODUCTS_METADATA:
        prod = {
            "product_id": p["id"],
            "product_name": p["name"],
            "brand": p["brand"],
            "category": p["category"],
            "subcategory": "Premium Tech",
            "price": float(p["base_price"]),
            "currency": "VND",
            "rating": round(random.uniform(4.1, 4.9), 1),
            "review_count": random.randint(150, 4500),
            "seller": f"{p['brand']} Official Store",
            "product_url": f"https://shop.sentinel.ai/p/{p['id']}",
            "image_url": f"https://assets.sentinel.ai/images/{p['id']}.jpg",
            "source": "catalog_sync",
            "created_at": (now - datetime.timedelta(days=60)).isoformat() + "Z",
            "updated_at": now.isoformat() + "Z"
        }
        products.append(prod)
    return products

def generate_reviews(products: List[Dict[str, Any]], count: int = 1500, inject_anomalies: bool = True) -> List[Dict[str, Any]]:
    reviews = []
    start_date = datetime.datetime(2026, 8, 20, 0, 0, 0)
    
    for i in range(count):
        prod = random.choice(products)
        delta_seconds = random.randint(0, 35 * 86400)
        review_time = start_date + datetime.timedelta(seconds=delta_seconds)
        
        is_vi = random.random() < 0.65
        lang = "vi" if is_vi else "en"
        
        # Rating bias towards 4 and 5 stars
        rating_roll = random.random()
        if rating_roll < 0.60:
            rating = 5.0
            text = random.choice(POSITIVE_FEEDBACK_VI if is_vi else POSITIVE_FEEDBACK_EN)
            title = "Rất hài lòng" if is_vi else "Great purchase"
        elif rating_roll < 0.85:
            rating = 4.0
            text = random.choice(POSITIVE_FEEDBACK_VI if is_vi else POSITIVE_FEEDBACK_EN)
            title = "Tốt trong tầm giá" if is_vi else "Good overall"
        elif rating_roll < 0.93:
            rating = 3.0
            text = "Dùng tạm được, chưa thực sự ấn tượng." if is_vi else "Decent, but expected more."
            title = "Tạm ổn" if is_vi else "Average"
        elif rating_roll < 0.97:
            rating = 2.0
            text = random.choice(NEGATIVE_FEEDBACK_VI if is_vi else NEGATIVE_FEEDBACK_EN)
            title = "Khá thất vọng" if is_vi else "Disappointed"
        else:
            rating = 1.0
            text = random.choice(NEGATIVE_FEEDBACK_VI if is_vi else NEGATIVE_FEEDBACK_EN)
            title = "Không hài lòng" if is_vi else "Poor quality"

        review = {
            "review_id": f"rv_{i+1:06d}",
            "product_id": prod["product_id"],
            "user_id": f"usr_{random.randint(1000, 9999)}",
            "rating": rating,
            "review_title": title,
            "review_text": text,
            "review_date": review_time.isoformat() + "Z",
            "verified_purchase": random.random() < 0.88,
            "helpful_count": random.randint(0, 45),
            "source": random.choice(["web_crawler", "mobile_app", "partner_api"]),
            "language": lang,
            "created_at": review_time.isoformat() + "Z"
        }
        reviews.append(review)

    # Inject specific anomalies as specified in plan.md
    if inject_anomalies:
        # Anomaly 1: Review Burst on iPhone 15 on Sep 20 (60 reviews in 1 hour)
        burst_time = datetime.datetime(2026, 9, 20, 14, 0, 0)
        for b in range(60):
            reviews.append({
                "review_id": f"rv_burst_{b+1:03d}",
                "product_id": "iphone_15",
                "user_id": f"usr_burst_{b+1}",
                "rating": 1.0 if b % 2 == 0 else 2.0,
                "review_title": "Lỗi màn hình sau update",
                "review_text": "Sau bản cập nhật iOS mới màn hình bị chớp xanh và nóng ran!",
                "review_date": (burst_time + datetime.timedelta(minutes=b)).isoformat() + "Z",
                "verified_purchase": True,
                "helpful_count": random.randint(10, 80),
                "source": "mobile_app",
                "language": "vi",
                "created_at": (burst_time + datetime.timedelta(minutes=b)).isoformat() + "Z"
            })

    return reviews

def generate_price_history(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prices = []
    start_date = datetime.datetime(2026, 8, 20, 0, 0, 0)
    price_idx = 1
    
    for prod in products:
        base = prod["price"]
        # Generate daily snapshots
        for day in range(35):
            cur_time = start_date + datetime.timedelta(days=day, hours=10)
            # Slight natural fluctuation +/- 3%
            fluctuation = random.uniform(-0.03, 0.03)
            current_price = round(base * (1.0 + fluctuation), -4)
            
            # Anomaly injection for galaxy_s24_ultra: sudden glitch drop on day 22
            if prod["product_id"] == "galaxy_s24_ultra" and day == 22:
                current_price = round(base * 0.15, -4) # 85% discount glitch

            prices.append({
                "price_id": f"pr_{price_idx:06d}",
                "product_id": prod["product_id"],
                "price": float(current_price),
                "currency": "VND",
                "seller": prod["seller"],
                "timestamp": cur_time.isoformat() + "Z",
                "source": "price_poller"
            })
            price_idx += 1
            
    return prices

def generate_streaming_events(products: List[Dict[str, Any]], count: int = 500) -> List[Dict[str, Any]]:
    events = []
    base_time = datetime.datetime(2026, 9, 24, 12, 0, 0)
    for i in range(count):
        prod = random.choice(products)
        event_time = base_time + datetime.timedelta(seconds=i * 2)
        event_type = random.choice(["NEW_REVIEW", "PRICE_UPDATE", "RATING_UPDATE"])
        
        if event_type == "NEW_REVIEW":
            payload = {"rating": random.choice([4.0, 5.0, 5.0, 3.0]), "review_id": f"rv_stream_{i:04d}"}
        elif event_type == "PRICE_UPDATE":
            payload = {"old_price": prod["price"], "new_price": round(prod["price"] * random.uniform(0.95, 1.05), -4)}
        else:
            payload = {"old_rating": prod["rating"], "new_rating": round(random.uniform(4.0, 5.0), 1)}

        events.append({
            "event_id": f"evt_{i+1:06d}",
            "event_type": event_type,
            "product_id": prod["product_id"],
            "timestamp": event_time.isoformat() + "Z",
            "source": "stream_simulator",
            "payload": payload
        })
    return events

if __name__ == "__main__":
    prods = generate_products()
    revs = generate_reviews(prods, count=1000)
    prs = generate_price_history(prods)
    evts = generate_streaming_events(prods, count=300)
    print(f"Generated {len(prods)} products, {len(revs)} reviews, {len(prs)} price points, {len(evts)} events.")
