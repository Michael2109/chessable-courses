from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List, Optional, Sequence

from app.puzzles import build_puzzles_pipeline, write_puzzles_per_theme_to_directory

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stream Lichess puzzles CSV, filter by rating, group by theme, "
            "and export a single PGN for annotation/import."
        )
    )

    # Prefer the project root's CSV if present (works when running from app/)
    project_root = Path(__file__).resolve().parents[1]
    cwd = Path.cwd()
    default_csv_candidates = [
        project_root / "lichess_db_puzzle.csv",
        project_root / "lichess_db_puzzle.csv.zst",
        cwd / "lichess_db_puzzle.csv",
        cwd / "lichess_db_puzzle.csv.zst",
    ]
    default_csv_path = next((str(p) for p in default_csv_candidates if p.exists()), str(cwd / "lichess_db_puzzle.csv"))

    parser.add_argument(
        "--csv-path",
        default=default_csv_path,
        help="Path to lichess_db_puzzle.csv or .csv.zst",
    )
    parser.add_argument(
        "--min-rating",
        type=int,
        default=600,
        help="Minimum rating (inclusive)",
    )
    parser.add_argument(
        "--max-rating",
        type=int,
        default=800,
        help="Maximum rating (inclusive)",
    )
    parser.add_argument(
        "--center",
        type=int,
        default=700,
        help=(
            "Difficulty center for sorting (distance from this rating). "
            "Use 700 for a 600-800 band."
        ),
    )
    parser.add_argument(
        "--per-theme",
        type=int,
        default=50,
        help="Select up to N puzzles per theme",
    )
    parser.add_argument(
        "--include-theme",
        action="append",
        default=None,
        help=(
            "Restrict to specific themes. Can be specified multiple times. "
            "Example: --include-theme mate --include-theme fork"
        ),
    )
    parser.add_argument(
        "--limit-total",
        type=int,
        default=None,
        help="Optional hard cap on total number of puzzles",
    )
    parser.add_argument(
        "--out-pgn",
        default=os.path.join(os.getcwd(), "puzzles_600_800.pgn"),
        help="Output PGN path",
    )
    parser.add_argument(
        "--start-after-first-move",
        action="store_true",
        default=True,
        help=(
            "Present the position after applying the first move from the CSV (solver to move). "
            "Defaults to ON to make FEN reflect the player's turn."
        ),
    )
    parser.add_argument(
        "--force-color",
        choices=["white", "black"],
        default=None,
        help=(
            "Optionally restrict to puzzles where the solver to move is this color "
            "(after applying --start-after-first-move if set)."
        ),
    )
    parser.add_argument(
        "--opening-color-tag",
        choices=["white", "black", "both"],
        default=None,
        help=(
            "Add an OpeningColor PGN tag to hint course color to platforms like Chessable "
            "(white/black/both)."
        ),
    )

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    # Parse CLI arguments (defaults applied when argv is None)
    args = parse_args(argv)

    written, summary, selected = build_puzzles_pipeline(
        csv_path=args.csv_path,
        rating_min=args.min_rating,
        rating_max=args.max_rating,
        per_theme=args.per_theme,
        difficulty_center=args.center,
        include_themes=args.include_theme,
        out_pgn_path=args.out_pgn,
        present_after_opponent_first_move=args.start_after_first_move,
        limit_total=args.limit_total,
        force_color=args.force_color,
        opening_color_tag=args.opening_color_tag,
    )

    print(f"Wrote {written} puzzles to {args.out_pgn}")
    # Human-friendly, concise console summary
    try:
        from itertools import islice

        ratings = summary.get("ratings", {})
        colors = summary.get("color_to_move", {})
        themes = list(summary.get("themes", []))

        print("\nSummary:")
        print(
            f"- Total: {summary.get('total', 0)} puzzles across {summary.get('theme_count', 0)} themes"
        )
        if ratings:
            print(
                f"- Ratings: min {ratings.get('min', '-')}, max {ratings.get('max', '-')}, "
                f"avg {ratings.get('avg', 0):.1f}, median {ratings.get('median', 0):.1f}"
            )
        if colors:
            print(
                f"- Start side to move: White {colors.get('white', 0)}, Black {colors.get('black', 0)}"
            )
        if themes:
            print("- Themes (count, avg rating, avg pop):")
            for t in themes:
                print(
                    f"  - {t.get('theme', '')}: {t.get('count', 0)}, "
                    f"{t.get('rating_avg', 0):.1f}, {t.get('pop_avg', 0):.1f}"
                )
        print()
    except Exception:
        pass

    # Emit per-theme PGNs under ./themes_pgn
    try:
        out_dir = os.path.join(os.getcwd(), "themes_pgn")
        counts = write_puzzles_per_theme_to_directory(
            puzzles_with_theme=selected,
            output_dir=out_dir,
            present_after_opponent_first_move=args.start_after_first_move,
            opening_color_tag=args.opening_color_tag,
        )
        print(f"Wrote per-theme PGNs to {out_dir}")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

