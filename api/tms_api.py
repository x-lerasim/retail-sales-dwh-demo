"""
TMS API — имитатор API транспортной компании.
Читает shipments из PostgreSQL → генерирует tracking events (GPS, температура, скорость).

GET /api/v1/tracking?since=2025-03-14T00:00:00&limit=100&page=1
GET /api/v1/tracking?shipment_id=SHP-A1B2C3D4
GET /api/v1/tracking/alerts?since=2025-03-14T00:00:00
GET /health
"""

import os
import random
import hashlib
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from contextlib import asynccontextmanager

# ── Config ───────────────────────────────────────────────────

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "admin")
PG_DB = os.getenv("PG_DB", "kmk")

EVENTS_CACHE: list[dict] = []


# ── Маршруты Татарстана (реальные координаты) ────────────────

ROUTES = {
    "Набережные Челны": [
        (55.7943, 49.1115, "Склад КМК, Казань"),
        (55.7522, 49.2058, "Казань, выезд на М7"),
        (55.6500, 49.9500, "Мамадыш, трасса М7"),
        (55.6800, 50.8200, "Елабуга"),
        (55.7435, 52.4055, "Набережные Челны, РЦ"),
    ],
    "Нижнекамск": [
        (55.7943, 49.1115, "Склад КМК, Казань"),
        (55.7522, 49.2058, "Казань, выезд на М7"),
        (55.6500, 49.9500, "Мамадыш"),
        (55.6340, 51.8160, "Нижнекамск"),
    ],
    "Альметьевск": [
        (55.7943, 49.1115, "Склад КМК, Казань"),
        (55.3900, 50.3100, "Чистополь"),
        (54.9013, 52.3155, "Альметьевск, РЦ"),
    ],
    "Зеленодольск": [
        (55.7943, 49.1115, "Склад КМК, Казань"),
        (55.8439, 48.5178, "Зеленодольск"),
    ],
}

DEFAULT_ROUTE = [
    (55.7943, 49.1115, "Склад КМК, Казань"),
    (55.7800, 49.1500, "Казань, в пути"),
    (55.7700, 49.1800, "Точка доставки"),
]


# ── Helpers ──────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS,
        dbname=PG_DB,
    )


def stable_seed(shipment_id: str, idx: int):
    """Один и тот же shipment всегда даёт одни и те же events."""
    h = hashlib.md5(f"{shipment_id}:{idx}".encode()).hexdigest()
    random.seed(int(h[:8], 16))


def pick_route(delivery_address: str) -> list[tuple]:
    if not delivery_address:
        return DEFAULT_ROUTE
    for city, route in ROUTES.items():
        if city in delivery_address:
            return route
    return DEFAULT_ROUTE


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def generate_events(ship: dict) -> list[dict]:
    """5-15 tracking events для одной отгрузки."""

    sid = ship["shipment_id"]
    shipped_at = ship["shipped_at"]
    if not shipped_at:
        return []

    end = ship["actual_arrival"] or ship["estimated_arrival"] or (shipped_at + timedelta(hours=6))
    route = pick_route(ship["delivery_address"] or "")
    total_min = max(60, int((end - shipped_at).total_seconds() / 60))
    n_events = min(15, max(5, total_min // 30))

    events = []

    for i in range(n_events):
        stable_seed(sid, i)
        progress = i / max(1, n_events - 1)
        ts = shipped_at + timedelta(minutes=int(total_min * progress))

        # позиция на маршруте
        route_pos = progress * (len(route) - 1)
        seg = min(int(route_pos), len(route) - 2)
        t = route_pos - seg
        lat = lerp(route[seg][0], route[seg + 1][0], t) + random.uniform(-0.003, 0.003)
        lon = lerp(route[seg][1], route[seg + 1][1], t) + random.uniform(-0.003, 0.003)

        # температура: норма 2-5°C, 3% шанс аномалии >6°C
        temp_breach = random.random() < 0.03
        temp = round(random.uniform(6.5, 9.5), 1) if temp_breach else round(random.uniform(1.5, 5.5), 1)

        speed = 0 if i == 0 or i == n_events - 1 else random.randint(35, 95)
        if random.random() < 0.08:
            speed = 0  # простой

        # тип события
        if i == 0:
            event_type = "pickup"
            checkpoint = route[0][2]
        elif i == n_events - 1:
            event_type = "delivered"
            checkpoint = route[-1][2]
        elif int(route_pos) != int((i - 1) / max(1, n_events - 1) * (len(route) - 1)):
            event_type = "checkpoint"
            checkpoint = route[min(seg + 1, len(route) - 1)][2]
        else:
            event_type = "temperature_reading"
            checkpoint = None

        # алерты
        alert = None
        alert_detail = None
        if temp_breach:
            alert = "temperature_breach"
            alert_detail = f"Температура {temp} превышает норму {ship.get('temperature_zone', '2-6')}"
        elif speed == 0 and i not in (0, n_events - 1) and random.random() < 0.3:
            alert = "long_stop"
            alert_detail = "Простой > 30 мин"

        event_id = f"TRK-{hashlib.md5(f'{sid}:{i}'.encode()).hexdigest()[:12].upper()}"

        events.append({
            "event_id": event_id,
            "shipment_id": sid,
            "order_id": ship["order_id"],
            "event_type": event_type,
            "timestamp": ts.isoformat(),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "temperature_celsius": temp,
            "speed_kmh": speed,
            "vehicle_plate": ship["vehicle_plate"],
            "carrier": ship["carrier"],
            "driver_name": ship["driver_name"],
            "checkpoint": checkpoint,
            "alert": alert,
            "alert_detail": alert_detail,
        })

    return events


def load_all_events():
    """Читает shipments из PG, генерирует events."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT shipment_id, order_id, delivery_address, status,
               carrier, vehicle_plate, driver_name,
               temperature_zone, shipped_at, estimated_arrival, actual_arrival
        FROM source_kmk.shipments
        WHERE shipped_at IS NOT NULL
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    all_events = []
    for row in rows:
        all_events.extend(generate_events(dict(row)))

    all_events.sort(key=lambda e: e["timestamp"])
    return all_events


# ── FastAPI ──────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(application: FastAPI):
    global EVENTS_CACHE
    print("Loading shipments from PostgreSQL...")
    EVENTS_CACHE = load_all_events()
    print(f"Generated {len(EVENTS_CACHE)} tracking events")
    yield

app = FastAPI(
    title="KMK TMS Tracking API",
    description="API транспортной компании — трекинг рефрижераторов, GPS, температура",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {"status": "ok", "events_count": len(EVENTS_CACHE)}


@app.get("/api/v1/tracking")
def get_tracking(
    since: Optional[str] = Query(None, description="ISO timestamp, например 2025-03-14T00:00:00"),
    until: Optional[str] = Query(None, description="ISO timestamp"),
    shipment_id: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None, description="pickup | in_transit | checkpoint | temperature_reading | delivered"),
    limit: int = Query(100, ge=1, le=1000),
    page: int = Query(1, ge=1),
):
    filtered = EVENTS_CACHE

    if since:
        filtered = [e for e in filtered if e["timestamp"] >= since]
    if until:
        filtered = [e for e in filtered if e["timestamp"] < until]
    if shipment_id:
        filtered = [e for e in filtered if e["shipment_id"] == shipment_id]
    if event_type:
        filtered = [e for e in filtered if e["event_type"] == event_type]

    total = len(filtered)
    start = (page - 1) * limit
    page_data = filtered[start:start + limit]

    return {
        "data": page_data,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit,
        },
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/v1/tracking/alerts")
def get_alerts(
    since: Optional[str] = Query(None),
    until: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    page: int = Query(1, ge=1),
):
    filtered = [e for e in EVENTS_CACHE if e["alert"] is not None]

    if since:
        filtered = [e for e in filtered if e["timestamp"] >= since]
    if until:
        filtered = [e for e in filtered if e["timestamp"] < until]

    total = len(filtered)
    start = (page - 1) * limit
    page_data = filtered[start:start + limit]

    return {
        "data": page_data,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit,
        },
        "timestamp": datetime.utcnow().isoformat(),
    }
