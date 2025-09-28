def profile_to_text(profile_obj) -> str:
    mapping = {
        "is_timeseries": "• 시계열 분석 필요",
        "is_aggregation": "• 집계 함수 필요",
        "has_filter": "• WHERE 조건 필요",
        "is_grouped": "• GROUP BY 필요",
        "has_ranking": "• 정렬/순위 필요",
        "has_temporal_comparison": "• 기간 비교 필요",
    }
    bullets = [
        text for field, text in mapping.items() if getattr(profile_obj, field, False)
    ]
    intent = getattr(profile_obj, "intent_type", None)
    if intent:
        bullets.append(f"• 의도 유형 → {intent}")

    return "\n".join(bullets)
