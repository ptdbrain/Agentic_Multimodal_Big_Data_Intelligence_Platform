"""Causal statistical evaluations: only observed past data, never future or filled gaps."""

from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import timedelta
import hashlib, json, math, statistics
from analytics.basic import _currency
from serving.store import instant, identifier


@dataclass(frozen=True)
class StatisticalConfig:
    history_hours: int = 24
    min_windows: int = 12
    burst_multiplier: float = 3.0
    min_burst: int = 5
    bombing_rating: float = 2.5
    rating_drop: float = 0.5
    min_current_reviews: int = 3
    zero_std_multiplier: float = 2.0
    price_days: int = 7
    price_min_points: int = 3
    price_zscore: float = 3.0
    price_iqr: float = 1.5
    price_constant_change: float = 0.10

    def __post_init__(self):
        for name in (
            "history_hours",
            "min_windows",
            "min_burst",
            "min_current_reviews",
            "price_days",
            "price_min_points",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError("Positive integer statistical configuration required")
        if self.min_windows < 2 or self.price_min_points < 2:
            raise ValueError("At least two baseline samples required")
        for name in (
            "burst_multiplier",
            "rating_drop",
            "zero_std_multiplier",
            "price_zscore",
            "price_iqr",
            "price_constant_change",
        ):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError("Finite positive threshold required")
        if not 1 <= self.bombing_rating <= 5:
            raise ValueError("Invalid bombing rating threshold")

    @classmethod
    def from_settings(cls):
        from config.settings import settings

        value = settings.analytics
        return cls(
            burst_multiplier=value.burst_threshold_multiplier,
            min_burst=value.burst_min_reviews,
            bombing_rating=value.bombing_rating_threshold,
            rating_drop=value.rating_drop_threshold,
            min_current_reviews=value.rating_drop_min_reviews,
            price_min_points=value.price_min_data_points,
            price_zscore=value.price_zscore_threshold,
            price_iqr=value.price_iqr_k,
        )

    @property
    def sha256(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()


def _evaluation(dataset, generation, product, start, end, rule, event_type, state, evidence, score=None):
    key = "|".join(
        [
            identifier(dataset),
            "causal-price" if rule.startswith("price-") else identifier(generation),
            str(product),
            start,
            rule,
        ]
    )
    return {
        "event_id": hashlib.sha256(key.encode()).hexdigest(),
        "product_id": product,
        "window_start": start,
        "window_end": end,
        "rule": rule,
        "event_type": event_type,
        "state": state,
        "score": score,
        "evidence": evidence,
    }


def stream_evaluations(
    rows,
    *,
    dataset_id: str,
    generation: str,
    watermark: str,
    config: StatisticalConfig | None = None,
    quality_status: str = "valid",
    candidate_from: str | None = None,
) -> list[dict]:
    config = config or StatisticalConfig.from_settings()
    water = instant(watermark)
    candidate_start = instant(candidate_from) if candidate_from is not None else None
    groups = defaultdict(list)
    sources = set()
    for source in rows:
        row = dict(source)
        row["_start"] = instant(row["window_start"])
        row["_end"] = instant(row["window_end"])
        if row["_end"] <= row["_start"] or (row["_end"] - row["_start"]).total_seconds() != row["window_seconds"]:
            raise ValueError("Window duration mismatch")
        if (
            type(row["review_count"]) is not int
            or row["review_count"] < 1
            or not math.isfinite(float(row["avg_rating"]))
            or not 1 <= float(row["avg_rating"]) <= 5
        ):
            raise ValueError("Invalid statistical window")
        sources.add(row["source_group"])
        groups[(row["product_id"], row["source_group"], row["window_seconds"])].append(row)
    if len(sources) > 1:
        raise ValueError("Separate source groups before evaluation")
    result = []
    for (product, source, seconds), values in groups.items():
        values.sort(key=lambda r: r["_start"])
        if len({r["_start"] for r in values}) != len(values):
            raise ValueError("Duplicate statistical window")
        for current in values:
            if candidate_start is not None and current["_start"] < candidate_start:
                continue
            history = [
                r
                for r in values
                if r["_end"] <= current["_start"]
                and r["_end"] <= water
                and r["_start"] >= current["_start"] - timedelta(hours=config.history_hours)
            ]
            count = [r["review_count"] for r in history]
            enough = quality_status == "valid" and len(history) >= config.min_windows
            mean = statistics.mean(count) if count else None
            baseline_rating = (
                sum(r["review_count"] * float(r["avg_rating"]) for r in history) / sum(count) if count else None
            )
            evidence = {
                "source_group": source,
                "detection_mode": "streaming",
                "quality_scope": "stream_contract",
                "quality_status": quality_status,
                "configuration": asdict(config),
                "configuration_sha256": config.sha256,
                "window_seconds": seconds,
                "baseline_windows": len(history),
                "baseline_samples": sum(count),
                "baseline_start": history[0]["_start"].isoformat() if history else None,
                "baseline_end": history[-1]["_end"].isoformat() if history else None,
                "baseline_count_mean": mean,
                "baseline_rating": baseline_rating,
                "current_samples": current["review_count"],
                "current_rating": float(current["avg_rating"]),
                "provisional": current["_end"] > water,
                "baseline_method": "observed_closed_past_windows",
            }
            cutoff = (
                max(
                    config.min_burst,
                    mean * config.zero_std_multiplier
                    if len(count) > 1 and statistics.stdev(count) == 0
                    else mean + config.burst_multiplier * statistics.stdev(count),
                )
                if enough
                else None
            )
            traffic = "insufficient_data" if not enough else "firing" if current["review_count"] >= cutoff else "clear"
            event = "REVIEW_BOMBING" if float(current["avg_rating"]) <= config.bombing_rating else "REVIEW_BURST"
            result.append(
                _evaluation(
                    dataset_id,
                    generation,
                    product,
                    current["_start"].isoformat(),
                    current["_end"].isoformat(),
                    "traffic",
                    event,
                    traffic,
                    {**evidence, "threshold": cutoff},
                    current["review_count"] / mean if enough and mean else None,
                )
            )
            delta = baseline_rating - float(current["avg_rating"]) if enough else None
            rating = (
                "insufficient_data"
                if not enough or current["review_count"] < config.min_current_reviews
                else "firing"
                if delta >= config.rating_drop
                else "clear"
            )
            result.append(
                _evaluation(
                    dataset_id,
                    generation,
                    product,
                    current["_start"].isoformat(),
                    current["_end"].isoformat(),
                    "rating",
                    "RATING_DROP",
                    rating,
                    {**evidence, "threshold": config.rating_drop},
                    delta,
                )
            )
    return result


def price_evaluations(
    rows, *, dataset_id: str, generation: str, config: StatisticalConfig | None = None, quality_status: str = "valid"
) -> list[dict]:
    from datetime import datetime, timezone, date

    config = config or StatisticalConfig.from_settings()
    groups = defaultdict(list)
    result = []
    sources = set()
    for item in rows:
        row = dict(item)
        sources.add(row.get("source_group", "unverified"))
        if len(sources) > 1:
            raise ValueError("Separate price source groups before evaluation")
        unit = _currency(row.get("currency"))
        try:
            day = date.fromisoformat(str(row["date"]))
        except (TypeError, ValueError) as error:
            raise ValueError("Expected a daily price date (YYYY-MM-DD)") from error
        try:
            point = datetime.combine(day, datetime.min.time(), timezone.utc)
            value = float(row["avg_price"])
        except (TypeError, ValueError):
            continue
        if unit is None or not math.isfinite(value) or value <= 0:
            continue
        groups[(row["product_id"], row.get("source_group", "unverified"), unit)].append((point, value))
    for (product, source, unit), observations in groups.items():
        observations.sort()
        if len({point for point, _ in observations}) != len(observations):
            raise ValueError("Duplicate price date")
        for point, value in observations:
            history = [
                (clock, price)
                for clock, price in observations
                if point - timedelta(days=config.price_days) <= clock < point
            ]
            baseline = [price for _, price in history]
            enough = quality_status == "valid" and len(baseline) >= config.price_min_points
            mean = statistics.mean(baseline) if baseline else None
            sd = statistics.stdev(baseline) if len(baseline) > 1 else None
            relative = abs(value - mean) / mean if mean else None
            z = (value - mean) / sd if enough and sd else None
            quartiles = statistics.quantiles(baseline, n=4, method="inclusive") if enough else None
            q1, q3 = (quartiles[0], quartiles[2]) if quartiles else (None, None)
            evidence = {
                "source_group": source,
                "detection_mode": "causal_batch",
                "baseline_method": "past_daily_price_observations",
                "currency": unit,
                "baseline_samples": len(baseline),
                "baseline_mean": mean,
                "baseline_std": sd,
                "baseline_start": history[0][0].isoformat() if history else None,
                "baseline_end": history[-1][0].isoformat() if history else None,
                "current_price": value,
                "configuration": asdict(config),
                "configuration_sha256": config.sha256,
                "quality_status": quality_status,
                "provisional": False,
            }
            zfire = enough and (
                abs(z) >= config.price_zscore if z is not None else relative >= config.price_constant_change
            )
            ifire = enough and (value < q1 - config.price_iqr * (q3 - q1) or value > q3 + config.price_iqr * (q3 - q1))
            for rule, event, firing, score in [
                ("price-z", "PRICE_ANOMALY", zfire, z if z is not None else relative),
                ("price-iqr", "PRICE_IQR_OUTLIER", ifire, relative),
            ]:
                result.append(
                    _evaluation(
                        dataset_id,
                        generation,
                        product,
                        point.isoformat(),
                        (point + timedelta(days=1)).isoformat(),
                        rule + ":" + unit,
                        event,
                        "insufficient_data" if not enough else "firing" if firing else "clear",
                        {
                            **evidence,
                            "score_method": "zscore" if z is not None and rule == "price-z" else "relative_change",
                        },
                        score,
                    )
                )
    return result
