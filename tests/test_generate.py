from __future__ import annotations

from pathlib import Path

from app.puzzles import GenerationConfig, generate_from_config

ROOT = Path(__file__).resolve().parents[1]
csv_path = ROOT / "lichess_db_puzzle.csv"

def test_generate_200_700(tmp_path: Path, monkeypatch) -> None:
    out_dir = ROOT / "themes_pgn_200-700"
    cfg = GenerationConfig(
        csv_path=str(csv_path),
        min_rating=200,
        max_rating=700,
        per_theme=5,
        center=450,
        include_themes=["advancedPawn"],
        out_dir=str(out_dir),
        event_prefix=None,
    )
    out = generate_from_config(cfg)
    assert out["out_dir"]
    assert Path(out["out_dir"]).exists()


def test_generate_500_1000_with_prefix(tmp_path: Path, monkeypatch) -> None:
    out_dir = ROOT / "themes_pgn_500-1000"
    cfg = GenerationConfig(
        csv_path=str(csv_path),
        min_rating=500,
        max_rating=1000,
        per_theme=5,
        center=750,
        include_themes=["advancedPawn"],
        out_dir=str(out_dir),
        event_prefix="Test",
    )
    out = generate_from_config(cfg)
    assert out["out_dir"]
    assert Path(out["out_dir"]).exists()


