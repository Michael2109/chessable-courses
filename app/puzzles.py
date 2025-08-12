from __future__ import annotations

import csv
import io
import os
from collections import defaultdict
import heapq
import itertools
import math
import re
import hashlib
from dataclasses import dataclass
from typing import Any, DefaultDict, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import chess
import chess.pgn


@dataclass(frozen=True)
class Puzzle:
    """Represents a single Lichess puzzle record.

    Columns documented at https://database.lichess.org/#puzzles
    """

    puzzle_id: str
    fen: str
    moves_uci: Tuple[str, ...]
    rating: int
    rating_deviation: int
    popularity: int
    num_plays: int
    themes: Tuple[str, ...]
    game_url: str
    opening_tags: Tuple[str, ...]

    @property
    def primary_themes(self) -> Tuple[str, ...]:
        """Return themes excluding empty strings, normalized to snake-case-like tokens.

        The CSV already provides space-separated tokens like "mateIn2"; we keep as-is
        but strip whitespace and drop empties.
        """

        return tuple(t for t in self.themes if t)


@dataclass(frozen=True)
class GenerationConfig:
    csv_path: str
    min_rating: int
    max_rating: int
    per_theme: int = 50
    center: Optional[int] = None
    include_themes: Optional[Sequence[str]] = None
    out_dir: Optional[str] = None
    out_pgn: Optional[str] = None
    start_after_first_move: bool = True
    force_color: Optional[str] = None
    opening_color_tag: Optional[str] = None
    event_prefix: Optional[str] = None


def _open_text_stream(path: str) -> io.TextIOBase:
    """Open a text stream for CSV which might be plain .csv or .csv.zst.

    Uses standard open for .csv. If the file ends with .zst, attempts to stream
    decompress using the optional 'zstandard' package.
    """

    if path.endswith(".zst"):
        try:
            import zstandard as zstd  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Reading .zst requires the 'zstandard' package. Install it or "
                "decompress the file first."
            ) from exc

        fh = open(path, "rb")
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding="utf-8", newline="")
        # Caller is responsible for closing returned stream. We rely on GC to close fh via wrapper.
        return text_stream
    else:
        return open(path, "r", encoding="utf-8", newline="")


CSV_HEADERS = (
    "PuzzleId",
    "FEN",
    "Moves",
    "Rating",
    "RatingDeviation",
    "Popularity",
    "NbPlays",
    "Themes",
    "GameUrl",
    "OpeningTags",
)


def _humanize_theme(name: str) -> str:
    if not name:
        return ""
    spaced = re.sub(r"([a-z])([A-Z0-9])", r"\1 \2", name)
    return spaced.replace("_", " ").strip().title()


def _stable_noise(puzzle_id: str, salt: str = "sel_v1") -> float:
    """Deterministic pseudo-random in [-0.5, 0.5) per puzzle id."""
    h = hashlib.sha1((salt + puzzle_id).encode("utf-8")).digest()[:4]
    val = int.from_bytes(h, byteorder="big")
    return (val / 0xFFFFFFFF) - 0.5


def iter_puzzles_csv(
    path: str,
    required_columns: Sequence[str] = CSV_HEADERS,
) -> Iterator[Puzzle]:
    """Stream puzzles from a CSV or .csv.zst file, yielding one Puzzle at a time.

    The function uses near-constant memory and validates the header order minimally.
    """

    with _open_text_stream(path) as text_stream:
        reader = csv.reader(text_stream)
        try:
            header = next(reader)
        except StopIteration:
            return

        # Validate header minimalistically
        missing = [c for c in required_columns if c not in header]
        if missing:
            raise ValueError(f"CSV is missing required columns: {missing}")

        # For efficiency, map indices once.
        index_of: Dict[str, int] = {name: header.index(name) for name in required_columns}

        def _normalize_uci(token: str) -> str:
            t = token.strip()
            if not t:
                return t
            # Normalize promotion forms like e7e8=Q -> e7e8q
            if len(t) == 6 and t[4] == "=":
                return (t[:4] + t[5].lower())
            return t.lower()

        for row in reader:
            if not row or len(row) < len(index_of):
                continue
            try:
                moves_field = row[index_of["Moves"]].strip()
                moves_uci = tuple(_normalize_uci(m) for m in moves_field.split() if m)
                themes_field = row[index_of["Themes"]].strip()
                themes = tuple(t for t in themes_field.split() if t)
                opening_field = row[index_of["OpeningTags"]].strip()
                opening_tags = tuple(t for t in opening_field.split() if t)

                yield Puzzle(
                    puzzle_id=row[index_of["PuzzleId"]].strip(),
                    fen=row[index_of["FEN"]].strip(),
                    moves_uci=moves_uci,
                    rating=int(row[index_of["Rating"]]),
                    rating_deviation=int(row[index_of["RatingDeviation"]]),
                    popularity=int(row[index_of["Popularity"]]),
                    num_plays=int(row[index_of["NbPlays"]]),
                    themes=themes,
                    game_url=row[index_of["GameUrl"]].strip(),
                    opening_tags=opening_tags,
                )
            except Exception:
                # Skip malformed lines
                continue


def filter_puzzles_by_rating(
    puzzles: Iterable[Puzzle], min_rating: int, max_rating: int
) -> Iterator[Puzzle]:
    for puzzle in puzzles:
        if min_rating <= puzzle.rating <= max_rating:
            yield puzzle


def group_puzzles_by_theme(puzzles: Iterable[Puzzle]) -> Dict[str, List[Puzzle]]:
    grouped: DefaultDict[str, List[Puzzle]] = defaultdict(list)
    for puzzle in puzzles:
        for theme in puzzle.primary_themes:
            grouped[theme].append(puzzle)
    return dict(grouped)


def sort_puzzles(
    puzzles: List[Puzzle],
    difficulty_center: Optional[int] = None,
) -> List[Puzzle]:
    """Sort by popularity desc, then by difficulty.

    If difficulty_center is provided (e.g., 700), sort by absolute distance from
    this center ascending; otherwise sort by rating ascending.
    """

    def sort_key(p: Puzzle) -> Tuple[int, int, int]:
        popularity_key = -p.popularity
        if difficulty_center is not None:
            difficulty_key = abs(p.rating - difficulty_center)
        else:
            difficulty_key = p.rating
        # Tie-breaker: lower deviation, then more plays (desc)
        return (popularity_key, difficulty_key, -p.num_plays)

    return sorted(puzzles, key=sort_key)


def _goodness_key(p: Puzzle, difficulty_center: Optional[int]) -> Tuple[float, float, float]:
    """Popularity-biased score with jitter to avoid always picking only the most popular."""
    jitter = _stable_noise(p.puzzle_id)
    popularity_key = float(p.popularity) + 10.0 * jitter
    if difficulty_center is not None:
        difficulty_key = -float(abs(p.rating - difficulty_center)) + 2.0 * jitter
    else:
        difficulty_key = -float(p.rating) + 2.0 * jitter
    plays_key = math.log10(max(1, p.num_plays)) + 0.5 * jitter
    return (popularity_key, difficulty_key, plays_key)


def select_top_per_theme_streaming(
    puzzles: Iterable[Puzzle],
    top_n_per_theme: int,
    difficulty_center: Optional[int] = None,
    include_themes: Optional[Sequence[str]] = None,
) -> List[Tuple[str, Puzzle]]:
    """Pick up to N best puzzles per theme while streaming input.

    Maintains a min-heap per theme keyed by a 'goodness' tuple so that the
    smallest element in each heap is the current worst kept; when a better
    candidate arrives and the heap is full, we replace the worst.
    """

    include_set = set(include_themes) if include_themes is not None else None
    per_theme_heaps: Dict[str, List[Tuple[Tuple[float, float, float], int, Puzzle]]] = {}
    tie_counter = itertools.count()

    for puzzle in puzzles:
        if not puzzle.primary_themes:
            continue
        for theme in puzzle.primary_themes:
            if include_set is not None and theme not in include_set:
                continue
            heap = per_theme_heaps.setdefault(theme, [])
            key = _goodness_key(puzzle, difficulty_center)
            entry = (key, next(tie_counter), puzzle)
            if len(heap) < top_n_per_theme:
                heapq.heappush(heap, entry)
            else:
                # If new entry is better (key greater) than current worst (heap[0])
                if entry[0] > heap[0][0]:
                    heapq.heapreplace(heap, entry)

    # Flatten, sorting each theme's heap from best to worst
    selected: List[Tuple[str, Puzzle]] = []
    for theme, heap in per_theme_heaps.items():
        # Sort descending by key to get best first
        for _, _, puzzle in sorted(heap, key=lambda e: e[0], reverse=True):
            selected.append((theme, puzzle))
    return selected


def _rating_bin_index(rating: int, min_rating: int, max_rating: int, bin_size: int) -> int:
    if rating < min_rating:
        return 0
    if rating > max_rating:
        return (max_rating - min_rating) // bin_size
    return (rating - min_rating) // bin_size


def select_top_per_theme_streaming_stratified(
    puzzles: Iterable[Puzzle],
    top_n_per_theme: int,
    min_rating: int,
    max_rating: int,
    bin_size: int = 5,
    difficulty_center: Optional[int] = None,
    include_themes: Optional[Sequence[str]] = None,
    present_after_opponent_first_move: bool = False,
) -> List[Tuple[str, Puzzle]]:
    """Streaming selection with even distribution across rating bins within each theme.

    Keeps a small heap per theme per rating bin. At the end, selects puzzles
    by round-robin across bins to approach an even spread across the band.
    """

    include_set = set(include_themes) if include_themes is not None else None
    num_bins = max(1, math.ceil((max_rating - min_rating + 1) / bin_size))
    per_bin_quota = max(1, math.ceil(top_n_per_theme / num_bins))

    # theme -> bin_index -> heap of size per_bin_quota
    per_theme_bin_heaps: Dict[str, Dict[int, List[Tuple[Tuple[float, float, float], int, Puzzle]]]] = {}
    tie_counter = itertools.count()

    for puzzle in puzzles:
        if not puzzle.primary_themes:
            continue
        bin_index = _rating_bin_index(puzzle.rating, min_rating, max_rating, bin_size)
        key = _goodness_key(puzzle, difficulty_center)
        entry = (key, next(tie_counter), puzzle)
        for theme in puzzle.primary_themes:
            if include_set is not None and theme not in include_set:
                continue
            bins = per_theme_bin_heaps.setdefault(theme, {})
            heap = bins.setdefault(bin_index, [])
            if len(heap) < per_bin_quota:
                heapq.heappush(heap, entry)
            else:
                if entry[0] > heap[0][0]:
                    heapq.heapreplace(heap, entry)

    def starts_white(p: Puzzle) -> bool:
        board = chess.Board(fen=p.fen)
        if present_after_opponent_first_move and p.moves_uci:
            try:
                board.push_uci(p.moves_uci[0])
            except Exception:
                # If illegal, just use original turn
                pass
        return board.turn == chess.WHITE

    # Round-robin selection across bins for each theme with color balancing
    selected: List[Tuple[str, Puzzle]] = []
    for theme, bins in per_theme_bin_heaps.items():
        count_for_theme = 0
        # Convert heaps to sorted lists descending by key (best first)
        bin_to_list: Dict[int, List[Tuple[Tuple[float, float, float], int, Puzzle]]] = {
            i: sorted(h, key=lambda e: e[0], reverse=True) for i, h in bins.items()
        }

        # Split each bin list into white/black start buckets
        bin_white: Dict[int, List[Puzzle]] = {}
        bin_black: Dict[int, List[Puzzle]] = {}
        for i, lst in bin_to_list.items():
            whites: List[Puzzle] = []
            blacks: List[Puzzle] = []
            for _, _, p in lst:
                (whites if starts_white(p) else blacks).append(p)
            bin_white[i] = whites
            bin_black[i] = blacks

        white_count = 0
        black_count = 0
        desired_white = True
        rr_start = 0

        # Round-robin over bin indices 0..num_bins-1 repeatedly
        while count_for_theme < top_n_per_theme:
            made_progress = False
            # Try desired color first
            for i in range(num_bins):
                bin_index = (rr_start + i) % num_bins
                if desired_white:
                    lstw = bin_white.get(bin_index)
                    if lstw:
                        puzzle = lstw.pop(0)
                        selected.append((theme, puzzle))
                        white_count += 1
                        count_for_theme += 1
                        made_progress = True
                        rr_start = (bin_index + 1) % num_bins
                        break
                else:
                    lstb = bin_black.get(bin_index)
                    if lstb:
                        puzzle = lstb.pop(0)
                        selected.append((theme, puzzle))
                        black_count += 1
                        count_for_theme += 1
                        made_progress = True
                        rr_start = (bin_index + 1) % num_bins
                        break

            # If nothing for desired color, try the other color
            if not made_progress:
                for i in range(num_bins):
                    bin_index = (rr_start + i) % num_bins
                    if desired_white:
                        lstb = bin_black.get(bin_index)
                        if lstb:
                            puzzle = lstb.pop(0)
                            selected.append((theme, puzzle))
                            black_count += 1
                            count_for_theme += 1
                            made_progress = True
                            rr_start = (bin_index + 1) % num_bins
                            break
                    else:
                        lstw = bin_white.get(bin_index)
                        if lstw:
                            puzzle = lstw.pop(0)
                            selected.append((theme, puzzle))
                            white_count += 1
                            count_for_theme += 1
                            made_progress = True
                            rr_start = (bin_index + 1) % num_bins
                            break

            if not made_progress:
                break

            # Update desired color to move toward balance
            desired_white = white_count <= black_count

    return selected


def write_puzzles_to_pgn(
    puzzles_with_theme: Iterable[Tuple[str, Puzzle]],
    output_path: str,
    present_after_opponent_first_move: bool = False,
    opening_color_tag: Optional[str] = None,
    event_prefix: Optional[str] = None,
) -> int:
    """Write puzzles to a single PGN file and return count written.

    Each puzzle becomes a separate PGN "game" with SetUp/FEN tags. Moves are
    applied from the CSV exactly as given. If present_after_opponent_first_move
    is True, we apply the first move (opponent move per Lichess spec), so the
    solver to move starts the sequence from the second move. By default this is
    disabled to show the solver as the side to move directly from the FEN.
    """

    games_written = 0
    per_theme_index: DefaultDict[str, int] = defaultdict(int)
    with open(output_path, "w", encoding="utf-8", newline="\n") as pgn_file:
        for theme, puzzle in puzzles_with_theme:
            if not puzzle.moves_uci:
                continue

            moves_sequence: Sequence[str] = puzzle.moves_uci
            board = chess.Board(fen=puzzle.fen)

            if present_after_opponent_first_move and len(moves_sequence) >= 1:
                try:
                    board.push_uci(moves_sequence[0])
                    moves_sequence = moves_sequence[1:]
                except ValueError:
                    # Skip if first move is invalid for the given FEN
                    continue


            game = chess.pgn.Game()
            # Group by theme via Event using a human-friendly name, with optional prefix
            friendly_theme = _humanize_theme(theme)
            if event_prefix:
                sep = "" if event_prefix.endswith(" ") else " "
                game.headers["Event"] = f"{event_prefix}{sep}{friendly_theme or theme}".strip()
            else:
                game.headers["Event"] = friendly_theme or theme
            # Use neutral site to avoid any accidental orientation hints from URLs like /black#
            game.headers["Site"] = "https://lichess.org"
            game.headers["SetUp"] = "1"
            game.headers["FEN"] = board.fen()
            # Non-standard orientation tags removed; viewers decide orientation
            game.headers["LichessURL"] = puzzle.game_url
            # Use side-to-move as primary label, and a compact ordinal for uniqueness
            # Name the student as the side to move
            if board.turn == chess.WHITE:
                game.headers["White"] = "You"
                game.headers["Black"] = "Opponent"
            else:
                game.headers["White"] = "Opponent"
                game.headers["Black"] = "You"
            game.headers["Result"] = "*"
            # Optional hint for platforms like Chessable to treat the course as a specific color
            if opening_color_tag in {"white", "black", "both"}:
                game.headers["OpeningColor"] = opening_color_tag
            game.headers["PuzzleId"] = puzzle.puzzle_id
            game.headers["Themes"] = " ".join(puzzle.primary_themes)
            per_theme_index[theme] += 1
            game.headers["Round"] = str(per_theme_index[theme])

            node = game
            try:
                for uci_move in moves_sequence:
                    move = chess.Move.from_uci(uci_move)
                    if move not in board.legal_moves:
                        # If it's illegal from this state, skip this puzzle
                        raise ValueError("Illegal move sequence for FEN")
                    board.push(move)
                    node = node.add_variation(move)
            except Exception:
                # Skip malformed sequences
                continue

            exporter = chess.pgn.FileExporter(pgn_file)
            game.accept(exporter)
            pgn_file.write("\n\n")
            games_written += 1

    return games_written


def write_puzzles_per_theme_to_directory(
    puzzles_with_theme: Sequence[Tuple[str, Puzzle]],
    output_dir: str,
    present_after_opponent_first_move: bool = False,
    opening_color_tag: Optional[str] = None,
    event_prefix: Optional[str] = None,
) -> Dict[str, int]:
    """Write one PGN file per theme; returns mapping theme -> count written.

    If event_prefix is provided, it will be prepended to the PGN Event header.
    """

    os.makedirs(output_dir, exist_ok=True)

    grouped: DefaultDict[str, List[Puzzle]] = defaultdict(list)
    for theme, puzzle in puzzles_with_theme:
        grouped[theme].append(puzzle)

    def sanitize(name: str) -> str:
        friendly = _humanize_theme(name) or name
        safe = re.sub(r"[^\w\- ]+", "_", friendly).strip()
        safe = re.sub(r"\s+", " ", safe)
        return safe

    out_counts: Dict[str, int] = {}
    for theme, puzzles in grouped.items():
        filename = sanitize(theme) + ".pgn"
        path = os.path.join(output_dir, filename)
        count = write_puzzles_to_pgn(
            puzzles_with_theme=[(theme, p) for p in puzzles],
            output_path=path,
            present_after_opponent_first_move=present_after_opponent_first_move,
            opening_color_tag=opening_color_tag,
            event_prefix=event_prefix,
        )
        out_counts[theme] = count

    return out_counts


def summarize_selected(
    puzzles_with_theme: Sequence[Tuple[str, Puzzle]],
    present_after_opponent_first_move: bool = False,
    top_themes: Optional[int] = None,
) -> Dict[str, Any]:
    """Produce a summary of the selected puzzles for logging/reporting."""

    total = len(puzzles_with_theme)
    if total == 0:
        return {
            "total": 0,
            "themes": [],
            "theme_count": 0,
            "ratings": {},
            "popularity": {},
            "color_to_move": {},
            "top_puzzles_by_popularity": [],
        }

    ratings = [p.rating for _, p in puzzles_with_theme]
    pops = [p.popularity for _, p in puzzles_with_theme]

    # Per-theme counts and aggregates
    theme_to_items: DefaultDict[str, List[Puzzle]] = defaultdict(list)
    for theme, puzzle in puzzles_with_theme:
        theme_to_items[theme].append(puzzle)

    themes_summary: List[Dict[str, Any]] = []
    for theme, items in theme_to_items.items():
        r = [x.rating for x in items]
        pp = [x.popularity for x in items]
        themes_summary.append(
            {
                "theme": theme,
                "count": len(items),
                "rating_avg": sum(r) / len(r),
                "rating_min": min(r),
                "rating_max": max(r),
                "pop_avg": sum(pp) / len(pp),
            }
        )

    themes_summary.sort(key=lambda t: (-t["count"], t["theme"]))
    if top_themes is not None:
        themes_summary = themes_summary[:top_themes]

    # Color-to-move distribution at start
    white_to_move = 0
    black_to_move = 0
    for _, p in puzzles_with_theme:
        board = chess.Board(fen=p.fen)
        if present_after_opponent_first_move and p.moves_uci:
            try:
                board.push_uci(p.moves_uci[0])
            except Exception:
                pass
        if board.turn == chess.WHITE:
            white_to_move += 1
        else:
            black_to_move += 1

    # Top puzzles by popularity
    top_by_pop = sorted(
        [p for _, p in puzzles_with_theme], key=lambda p: (-p.popularity, -p.num_plays)
    )[:10]

    def median(values: List[int]) -> float:
        s = sorted(values)
        n = len(s)
        mid = n // 2
        if n % 2 == 1:
            return float(s[mid])
        return (s[mid - 1] + s[mid]) / 2.0

    summary: Dict[str, Any] = {
        "total": total,
        "theme_count": len(theme_to_items),
        "themes": themes_summary,
        "ratings": {
            "min": min(ratings),
            "max": max(ratings),
            "avg": sum(ratings) / len(ratings),
            "median": median(ratings),
        },
        "popularity": {
            "min": min(pops),
            "max": max(pops),
            "avg": sum(pops) / len(pops),
        },
        "color_to_move": {
            "white": white_to_move,
            "black": black_to_move,
        },
        "top_puzzles_by_popularity": [
            {
                "puzzle_id": p.puzzle_id,
                "rating": p.rating,
                "popularity": p.popularity,
                "themes": " ".join(p.primary_themes),
            }
            for p in top_by_pop
        ],
    }

    return summary


def build_puzzles_pipeline(
    csv_path: str,
    rating_min: int,
    rating_max: int,
    per_theme: int,
    difficulty_center: Optional[int],
    include_themes: Optional[Sequence[str]],
    out_pgn_path: Optional[str],
    present_after_opponent_first_move: bool = False,
    limit_total: Optional[int] = None,
    force_color: Optional[str] = None,
    opening_color_tag: Optional[str] = None,
    event_prefix: Optional[str] = None,
) -> Tuple[int, Dict[str, Any], List[Tuple[str, Puzzle]]]:
    """End-to-end pipeline: stream, filter, group, select, and emit PGN.

    Returns the number of games written.
    """

    streamed = iter_puzzles_csv(csv_path)
    filtered = filter_puzzles_by_rating(streamed, rating_min, rating_max)
    # Default difficulty center to the midpoint of the band if not provided
    center_to_use: Optional[int]
    if difficulty_center is None:
        center_to_use = (int(rating_min) + int(rating_max)) // 2
    else:
        center_to_use = difficulty_center
    # Use stratified selection to spread across rating band in small increments
    selected = select_top_per_theme_streaming_stratified(
        filtered,
        top_n_per_theme=per_theme,
        min_rating=rating_min,
        max_rating=rating_max,
        bin_size=5,
        difficulty_center=center_to_use,
        include_themes=include_themes,
        present_after_opponent_first_move=present_after_opponent_first_move,
    )

    # Stable order: sort themes alphabetically, then keep per-theme order (already best-first within theme)
    selected.sort(key=lambda t: (t[0],))

    # Optionally force a uniform color for the solver to move by filtering
    if force_color in {"white", "black"}:
        def _is_white_after(p: Puzzle) -> bool:
            board = chess.Board(fen=p.fen)
            if present_after_opponent_first_move and p.moves_uci:
                try:
                    board.push_uci(p.moves_uci[0])
                except Exception:
                    pass
            return board.turn == chess.WHITE

        desired_white = (force_color == "white")
        selected = [(t, p) for (t, p) in selected if _is_white_after(p) == desired_white]

    if limit_total is not None:
        selected = selected[:limit_total]

    written = 0
    if out_pgn_path:
        # Ensure directory exists only if we are writing the combined PGN
        os.makedirs(os.path.dirname(out_pgn_path) or ".", exist_ok=True)
        written = write_puzzles_to_pgn(
            puzzles_with_theme=selected,
            output_path=out_pgn_path,
            present_after_opponent_first_move=present_after_opponent_first_move,
            opening_color_tag=opening_color_tag if opening_color_tag in {"white", "black", "both"} else None,
            event_prefix=event_prefix,
        )
    else:
        # If not writing a combined PGN, report how many were selected
        written = len(selected)

    summary = summarize_selected(
        puzzles_with_theme=selected,
        present_after_opponent_first_move=present_after_opponent_first_move,
    )

    return written, summary, selected


def generate_from_config(config: GenerationConfig) -> Dict[str, Any]:
    """High-level entry: generate packs based on a structured configuration.

    Returns a dictionary with keys:
      - selected: the list of (theme, Puzzle) selected
      - written_combined: count of games written to combined PGN (0 if none)
      - per_theme_counts: optional dict of theme->count written for per-theme PGNs
      - summary: summary stats
      - out_dir: directory where per-theme PGNs were written (if any)
      - out_pgn: path of combined PGN (if any)
    """

    written, summary, selected = build_puzzles_pipeline(
        csv_path=config.csv_path,
        rating_min=config.min_rating,
        rating_max=config.max_rating,
        per_theme=config.per_theme,
        difficulty_center=config.center,
        include_themes=config.include_themes,
        out_pgn_path=config.out_pgn,
        present_after_opponent_first_move=config.start_after_first_move,
        limit_total=None,
        force_color=config.force_color,
        opening_color_tag=config.opening_color_tag,
        event_prefix=config.event_prefix,
    )

    # Determine output directory for per-theme PGNs
    derived_dir = f"themes_pgn_{config.min_rating}-{config.max_rating}"
    out_dir = config.out_dir or os.path.join(os.getcwd(), derived_dir)
    per_theme_counts: Optional[Dict[str, int]] = None

    try:
        per_theme_counts = write_puzzles_per_theme_to_directory(
            puzzles_with_theme=selected,
            output_dir=out_dir,
            present_after_opponent_first_move=config.start_after_first_move,
            opening_color_tag=config.opening_color_tag,
            event_prefix=config.event_prefix,
        )
    except Exception:
        per_theme_counts = None

    return {
        "selected": selected,
        "written_combined": written,
        "per_theme_counts": per_theme_counts,
        "summary": summary,
        "out_dir": out_dir,
        "out_pgn": config.out_pgn,
    }

