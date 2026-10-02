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

from typing import List, Dict, Any, Optional
from config.settings import settings

def generate_products(
    metadata: Optional[List[Dict[str, Any]]] = None,
    reference_date: Optional[datetime.datetime] = None,
    currency: Optional[str] = None
) -> List[Dict[str, Any]]:
    products = []
    now = reference_date or datetime.datetime(2026, 9, 24, 8, 0, 0)
    meta_list = metadata or PRODUCTS_METADATA
    curr = currency or settings.analytics.default_currency

    for p in meta_list:
        prod = {
            "product_id": p["id"],
            "product_name": p["name"],
            "brand": p["brand"],
            "category": p["category"],
            "subcategory": p.get("subcategory", "Premium Tech"),
            "price": float(p["base_price"]),
            "currency": curr,
            "rating": round(random.uniform(4.1, 4.9), 1),
            "review_count": random.randint(150, 4500),
            "seller": f"{p['brand']} Official Store",
            "product_url": f"https://shop.sentinel.ai/p/{p['id']}",
            "image_url": f"https://assets.sentinel.ai/images/{p['id']}.jpg",
            "source": "catalog_sync",
            "created_at": (now - datetime.timedelta(days=60)).isoformat().replace("+00:00", "Z"),
            "updated_at": now.isoformat().replace("+00:00", "Z")
        }
        products.append(prod)
    return products

def generate_reviews(
    products: List[Dict[str, Any]],
    count: int = 1500,
    inject_anomalies: bool = True,
    start_date: Optional[datetime.datetime] = None,
    burst_product_id: Optional[str] = None,
    burst_count: int = 60
) -> List[Dict[str, Any]]:
    reviews = []
    base_start = start_date or datetime.datetime(2026, 8, 20, 0, 0, 0)
    target_burst_prod = burst_product_id or ("iphone_15" if any(p["product_id"] == "iphone_15" for p in products) else products[0]["product_id"])
    
    for i in range(count):
        prod = random.choice(products)
        delta_seconds = random.randint(0, 35 * 86400)
        review_time = base_start + datetime.timedelta(seconds=delta_seconds)
        
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
            "review_date": review_time.isoformat().replace("+00:00", "Z"),
            "verified_purchase": random.random() < 0.88,
            "helpful_count": random.randint(0, 45),
            "source": random.choice(["web_crawler", "mobile_app", "partner_api"]),
            "language": lang,
            "created_at": review_time.isoformat().replace("+00:00", "Z")
        }
        reviews.append(review)

    # Inject specific anomaly
    if inject_anomalies and target_burst_prod:
        burst_time = base_start + datetime.timedelta(days=31, hours=14)
        for b in range(burst_count):
            reviews.append({
                "review_id": f"rv_burst_{b+1:03d}",
                "product_id": target_burst_prod,
                "user_id": f"usr_burst_{b+1}",
                "rating": 1.0 if b % 2 == 0 else 2.0,
                "review_title": "Lỗi màn hình sau update",
                "review_text": "Sau bản cập nhật iOS mới màn hình bị chớp xanh và nóng ran!",
                "review_date": (burst_time + datetime.timedelta(minutes=b)).isoformat().replace("+00:00", "Z"),
                "verified_purchase": True,
                "helpful_count": random.randint(10, 80),
                "source": "mobile_app",
                "language": "vi",
                "created_at": (burst_time + datetime.timedelta(minutes=b)).isoformat().replace("+00:00", "Z")
            })

    return reviews

def generate_price_history(
    products: List[Dict[str, Any]],
    days_history: int = 35,
    start_date: Optional[datetime.datetime] = None,
    currency: Optional[str] = None
) -> List[Dict[str, Any]]:
    prices = []
    base_start = start_date or datetime.datetime(2026, 8, 20, 0, 0, 0)
    curr = currency or settings.analytics.default_currency
    price_idx = 1
    
    for prod in products:
        base = prod["price"]
        for day in range(days_history):
            cur_time = base_start + datetime.timedelta(days=day, hours=10)
            fluctuation = random.uniform(-0.03, 0.03)
            current_price = round(base * (1.0 + fluctuation), -4)
            
            # Anomaly injection for first product or galaxy_s24_ultra: sudden glitch drop
            is_anomaly_prod = prod["product_id"] == "galaxy_s24_ultra" or (prod == products[0] and len(products) == 1)
            if is_anomaly_prod and day == int(days_history * 0.65):
                current_price = round(base * 0.15, -4)

            prices.append({
                "price_id": f"pr_{price_idx:06d}",
                "product_id": prod["product_id"],
                "price": float(current_price),
                "currency": curr,
                "seller": prod.get("seller", "Official Store"),
                "timestamp": cur_time.isoformat().replace("+00:00", "Z"),
                "source": "price_poller"
            })
            price_idx += 1
            
    return prices

def generate_streaming_events(
    products: List[Dict[str, Any]],
    count: int = 500,
    base_time: Optional[datetime.datetime] = None
) -> List[Dict[str, Any]]:
    events = []
    start_t = base_time or datetime.datetime(2026, 9, 24, 12, 0, 0)
    for i in range(count):
        prod = random.choice(products)
        event_time = start_t + datetime.timedelta(seconds=i * 2)
        event_type = random.choice(["NEW_REVIEW", "PRICE_UPDATE", "RATING_UPDATE"])
        
        if event_type == "NEW_REVIEW":
            payload = {"rating": random.choice([4.0, 5.0, 5.0, 3.0]), "review_id": f"rv_stream_{i:04d}"}
        elif event_type == "PRICE_UPDATE":
            payload = {"old_price": prod["price"], "new_price": round(prod["price"] * random.uniform(0.95, 1.05), -4)}
        else:
            payload = {"old_rating": prod.get("rating", 4.5), "new_rating": round(random.uniform(4.0, 5.0), 1)}

        events.append({
            "event_id": f"evt_{i+1:06d}",
            "event_type": event_type,
            "product_id": prod["product_id"],
            "timestamp": event_time.isoformat().replace("+00:00", "Z"),
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

