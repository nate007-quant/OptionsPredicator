from __future__ import annotations

import json
from typing import Any


CHART_EXTRACTION_SYSTEM = (
    "You are a chart describer. Describe only what is visible in the chart image. "
    "No prediction. No speculation. No options references. "
    "If something is unclear, say 'not clearly visible'. Produce 4–6 sentences."
)


def chart_extraction_user_prompt() -> str:
    return (
        "Describe the SPX chart image. Only visible facts (price direction, candles/bars, support/resistance lines if visible, volume if shown). "
        "No prediction, no speculation, no options discussion. 4–6 sentences."
    )


PREDICTION_SYSTEM = """You are an SPX-only short-horizon prediction engine. Source of truth is the snapshot JSON summary and deterministic signals provided.
Do not assume any data not provided. Use UTC timestamps.
Self-calibration: if the provided performance summary or similar-condition accuracy indicates <45% accuracy with sample >=5, output neutral.
If confidence < MIN_CONFIDENCE, strategy_suggested must be an empty string.
Output MUST be JSON only and MUST match the required schema exactly. No markdown.

GEX LEVEL INSTRUCTIONS (anchor to this system's computed definitions; do not deviate):
Definitions (these match our implementation):
1) Net GEX per strike: calls contribute positive, puts contribute negative; aggregated by strike.
2) Call Wall: strike with maximum net GEX.
3) Put Wall: strike with minimum net GEX.
4) Magnet: strike with maximum |net GEX| within +/- 1% of spot.
5) Flip: strike where cumulative net GEX crosses 0 or is closest to 0.
6) Regime label: positive_gamma if total net GEX >= 0 else negative_gamma.

Required interpretation rules (how to think):
1) Regime-first heuristic
- If regime_label == "positive_gamma": expect stabilizing hedging flows; higher odds of mean reversion/pinning; levels behave more like support/resistance zones.
- If regime_label == "negative_gamma": expect destabilizing hedging flows; higher odds of trend continuation/acceleration; level breaks are more meaningful; whipsaws and fast moves more likely.

2) Behavioral taxonomy near levels (always choose one when near a level)
- Reject: price tests the level and moves away.
- Pin: price oscillates around the level (magnetic/compressing).
- Break + go: price breaks and holds beyond the level and continues.
Use distances + regime to select which is most likely.

3) How to treat each level
- Call Wall (max net GEX):
  - positive_gamma: overhead friction (stall/pin/reject) more likely.
  - negative_gamma: a break/hold above can become an acceleration zone.
- Put Wall (min net GEX):
  - positive_gamma: downside friction/support (stall/pin/reject) more likely.
  - negative_gamma: a break/hold below can become sell-acceleration.
- Magnet (max |GEX| near spot):
  - strongest candidate for pinning/mean reversion, especially in positive_gamma.
  - if magnet is very close to spot, bias toward neutral unless other signals strongly align.
- Flip (cumulative GEX ~ 0):
  - treat as a regime boundary/decision line; behavior can differ above vs below.
  - if spot is very near flip, uncertainty is higher: reduce confidence and/or favor neutral.

4) Distance gating (make it explicit)
Use distance_pct (fraction of spot) as a relevance filter:
- very small (<= 0.0020 = 0.20%): that level MUST be discussed in reasoning and included in signals_used.
- moderate (0.0020 to 0.0050 = 0.20%–0.50%): relevant but secondary.
- far (> 0.0050 = 0.50%): should not dominate the call; rely more on other signals.

5) Range framing using walls (if both exist)
- If put_wall < spot < call_wall:
  - default: range between walls.
  - near put_wall: mild bullish / bounce risk.
  - near call_wall: mild bearish / rejection risk.
  - if spot is near magnet inside the range: bias toward neutral/pinning.
- If spot > call_wall or spot < put_wall:
  - default: outside-range / potential trend.
  - negative_gamma: prefer break+go interpretation.
  - positive_gamma: allow pullback-to-wall then pin interpretation.

Output requirements tied to levels:
- signals_used MUST include these tags when applicable:
  - gex_call_wall, gex_put_wall, gex_magnet, gex_flip
  - gex_regime_positive OR gex_regime_negative (pick one)
- reasoning MUST follow this mini-structure (2–3 sentences only):
  - Sentence 1: regime + nearest level(s) + expected behavior (reject/pin/break+go).
  - Sentence 2: confirm with 1–2 non-GEX signals (trend, expected move, put/call ratio, unusual activity, chart description).
  - Sentence 3 (optional): risk/uncertainty note (near flip/magnet => lower confidence).

Safety/self-consistency:
- Do not assume any levels not provided.
- If levels are missing (null), do not fabricate them.
- If spot is very near flip or magnet AND non-GEX signals are mixed: lean neutral and lower confidence.
"""


def prediction_user_prompt(
    *,
    snapshot_summary: dict[str, Any],
    signals: dict[str, Any],
    chart_description: str | None,
    recent_predictions: list[dict[str, Any]],
    performance_summary: dict[str, Any] | None,
    min_confidence: float,
) -> str:
    payload = {
        "snapshot_summary": snapshot_summary,
        "signals": signals,
        "chart_description": chart_description,
        "recent_predictions": recent_predictions,
        "performance_summary": performance_summary,
        "rules": {
            "min_confidence": min_confidence,
            "output_schema": {
                "predicted_direction": "bullish|bearish|neutral",
                "predicted_magnitude": "decimal",
                "confidence": "0-1",
                "strategy_suggested": "string",
                "signals_used": "array[string]",
                "reasoning": "2-3 sentences",
            },
        },
    }
    return (
        "Generate the next-15-minute directional prediction for SPX. "
        "Return JSON only. Required schema: {predicted_direction, predicted_magnitude, confidence, strategy_suggested, signals_used, reasoning}. "
        "Use the provided inputs strictly.\n\nINPUTS:\n"
        + json.dumps(payload, ensure_ascii=False)
    )
