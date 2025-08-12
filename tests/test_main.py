from app.main import parse_args, main
from pathlib import Path
import os


def test_parse_args_defaults() -> None:
    args = parse_args([])
    assert args.min_rating == 600
    assert args.max_rating == 800
    # Combined PGN is now optional by default
    assert args.out_pgn is None


def test_packs_output_directory_derivation(tmp_path: Path, monkeypatch) -> None:
    # Arrange a temp CWD so the main CLI writes into it
    monkeypatch.chdir(tmp_path)
    # Make a tiny CSV so the run succeeds
    csv_path = tmp_path / "lichess_db_puzzle.csv"
    csv_path.write_text(
        """PuzzleId,FEN,Moves,Rating,RatingDeviation,Popularity,NbPlays,Themes,GameUrl,OpeningTags
id1,8/8/8/8/8/8/8/K6k w - - 0 1,a2a3,300,80,50,10,advancedPawn,https://lichess.org/abc,Test
id2,8/8/8/8/8/8/8/K6k w - - 0 1,a2a3,650,80,50,10,advancedPawn,https://lichess.org/def,Test
""",
        encoding="utf-8",
    )

    # Build range 200-700, expect dir themes_pgn_200-700
    exit_code = main([
        "--csv-path", str(csv_path),
        "--min-rating", "200",
        "--max-rating", "700",
        "--per-theme", "2",
        "--include-theme", "advancedPawn",
    ])
    assert exit_code == 0
    derived = tmp_path / "themes_pgn_200-700"
    assert derived.exists()

    # Build a second pack 500-1000 with event prefix and custom dir
    custom_dir = tmp_path / "themes_pgn_500-1000_custom"
    exit_code2 = main([
        "--csv-path", str(csv_path),
        "--min-rating", "500",
        "--max-rating", "1000",
        "--per-theme", "2",
        "--include-theme", "advancedPawn",
        "--event-prefix", "Test",
        "--out-dir", str(custom_dir),
    ])
    assert exit_code2 == 0
    assert custom_dir.exists()


