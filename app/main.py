from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List, Optional, Sequence

from app.puzzles import build_puzzles_pipeline, write_puzzles_per_theme_to_directory, process_all_puzzles_by_theme_streaming

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
        "--min-popularity",
        type=int,
        default=None,
        help="Minimum popularity percentile (e.g., 90 for 90th percentile)",
    )
    parser.add_argument(
        "--min-plays",
        type=int,
        default=None,
        help="Minimum number of plays/reviews (inclusive)",
    )
    parser.add_argument(
        "--center",
        type=int,
        default=None,
        help=(
            "Difficulty center for ordering. If omitted, defaults to the midpoint of "
            "[--min-rating, --max-rating]."
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
        default=None,
        help=(
            "Optional path for a single combined PGN. "
            "If omitted, no combined PGN is written; only per-theme PGNs are created."
        ),
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
    parser.add_argument(
        "--event-prefix",
        default=None,
        help="Optional text to prefix the PGN Event tag (e.g., 'Test').",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Directory to place per-theme PGNs. "
            "Defaults to a name derived from rating range, e.g., 'themes_pgn_200-700'."
        ),
    )
    parser.add_argument(
        "--stream-all",
        action="store_true",
        default=False,
        help=(
            "Process all puzzles by theme using streaming (memory-efficient). "
            "This will create one file per theme with all puzzles in that theme, "
            "sorted by difficulty. Ignores --per-theme and --limit-total."
        ),
    )
    parser.add_argument(
        "--include-difficulty",
        action="store_true",
        default=True,
        help="Include difficulty information in PGN event names (default: True)",
    )

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    # Parse CLI arguments (defaults applied when argv is None)
    args = parse_args(argv)

    if args.stream_all:
        # Use streaming processing for all puzzles
        print("Processing all puzzles by theme using streaming...")
        counts = process_all_puzzles_by_theme_streaming(
            csv_path=args.csv_path,
            min_rating=args.min_rating,
            max_rating=args.max_rating,
            min_popularity=args.min_popularity,
            min_plays=args.min_plays,
            output_dir=args.out_dir or f"themes_pgn_{args.min_rating}-{args.max_rating}",
            present_after_opponent_first_move=args.start_after_first_move,
            opening_color_tag=args.opening_color_tag,
            event_prefix=args.event_prefix,
            include_difficulty_in_event=args.include_difficulty,
        )
        
        print(f"\nProcessing complete!")
        print(f"Total themes processed: {len(counts)}")
        print(f"Total puzzles written: {sum(counts.values())}")
        
        # Show all themes by puzzle count
        sorted_themes = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        print(f"\nAll themes by puzzle count:")
        for theme, count in sorted_themes:
            print(f"  {theme}: {count} puzzles")
            
        return 0

    # Use the original pipeline for selective processing
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
        event_prefix=args.event_prefix,
        min_popularity=args.min_popularity,
        min_plays=args.min_plays,
    )

    if args.out_pgn:
        print(f"Wrote {written} puzzles to {args.out_pgn}")
    else:
        print(f"Selected {written} puzzles (no combined PGN written)")
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

    # Detailed per-theme listing ordered by rating (difficulty)
    try:
        from collections import defaultdict
        theme_to_puzzles: dict[str, list] = defaultdict(list)
        for theme, puzzle in selected:
            theme_to_puzzles[theme].append(puzzle)

        def difficulty_key(p):
            return (p.rating,)

        print("Details by theme (ordered by rating):")
        for theme in sorted(theme_to_puzzles.keys()):
            items = sorted(theme_to_puzzles[theme], key=difficulty_key)
            friendly = theme.replace("_", " ")
            print(f"\n[{friendly}] count={len(items)}")
            for idx, p in enumerate(items, start=1):
                dist = abs(p.rating - args.center) if isinstance(args.center, int) else 0
                print(
                    f"  {idx:>3}. id={p.puzzle_id} rating={p.rating} dev={p.rating_deviation} "
                    f"pop={p.popularity} plays={p.num_plays} dist={dist} url={p.game_url}"
                )
        print()
    except Exception:
        pass

    # Emit per-theme PGNs under a directory derived from settings (or user-provided)
    try:
        derived_dir = f"themes_pgn_{args.min_rating}-{args.max_rating}"
        out_dir = args.out_dir or os.path.join(os.getcwd(), derived_dir)
        print(f"Writing per-theme PGNs to {out_dir}...")
        counts = write_puzzles_per_theme_to_directory(
            puzzles_with_theme=selected,
            output_dir=out_dir,
            present_after_opponent_first_move=args.start_after_first_move,
            opening_color_tag=args.opening_color_tag,
            event_prefix=args.event_prefix,
            include_difficulty_in_event=args.include_difficulty,
        )
        print(f"✓ Wrote per-theme PGNs to {out_dir}")
    except Exception as e:
        print(f"Error writing per-theme PGNs: {e}")
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

