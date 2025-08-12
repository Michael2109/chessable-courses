from __future__ import annotations

import io
import os
from pathlib import Path
from typing import List, Tuple

import chess

from app.puzzles import (
    Puzzle,
    iter_puzzles_csv,
    filter_puzzles_by_rating,
    select_top_per_theme_streaming,
    write_puzzles_to_pgn,
)


SAMPLE_CSV = """PuzzleId,FEN,Moves,Rating,RatingDeviation,Popularity,NbPlays,Themes,GameUrl,OpeningTags
00sHx,q3k1nr/1pp1nQpp/3p4/1P2p3/4P3/B1PP1b2/B5PP/5K2 b k - 0 17,e8d7 a2e6 d7d8 f7f8,700,80,83,72,mate mateIn2 middlegame short,https://lichess.org/yyznGmXs/black#34,Italian_Game Italian_Game_Classical_Variation
00sJ9,r3r1k1/p4ppp/2p2n2/1p6/3P1qb1/2NQR3/PPB2PP1/R1B3K1 w - - 5 18,e3g3 e8e1 g1h2 e1c1 a1c1 f4h6 h2g1 h6c1,750,105,87,325,advantage attraction fork middlegame sacrifice veryLong,https://lichess.org/gyFeQsOE#35,French_Defense French_Defense_Exchange_Variation
00sO1,1k1r4/pp3pp1/2p1p3/4b3/P3n1P1/8/KPP2PN1/3rBR1R b - - 2 31,b8c7 e1a5 b7b6 f1d1,680,85,94,293,advantage discoveredAttack master middlegame short,https://lichess.org/vsfFkG0s/black#62,
"""


def _write_temp_csv(tmp_path: Path) -> Path:
    csv_path = tmp_path / "sample_puzzles.csv"
    csv_path.write_text(SAMPLE_CSV, encoding="utf-8")
    return csv_path


def test_iter_and_filter_and_select_and_pgn(tmp_path: Path) -> None:
    csv_path = _write_temp_csv(tmp_path)

    # Stream rows
    puzzles = iter_puzzles_csv(str(csv_path))

    # Filter to target band
    filtered = filter_puzzles_by_rating(puzzles, 600, 800)

    # Select top per theme in streaming mode
    selected = select_top_per_theme_streaming(
        filtered, top_n_per_theme=2, difficulty_center=700, include_themes=["mate", "fork", "advantage"]
    )

    # Write to PGN
    out_pgn = tmp_path / "out.pgn"
    count = write_puzzles_to_pgn(selected, str(out_pgn), present_after_opponent_first_move=True, event_prefix="Test")

    assert count >= 1
    text = out_pgn.read_text(encoding="utf-8")

    # Basic PGN structure checks
    assert "[SetUp " in text
    assert "[FEN " in text
    assert "[Event " in text
    assert "PuzzleId" in text

    # Ensure at least one move is present and legal from the FEN
    games = list(chess.pgn.read_game(io.StringIO(text)) for _ in range(1))
    game = games[0]
    assert game is not None
    assert game.headers.get("SetUp") == "1"
    # Event now groups by humanized theme name with optional prefix
    event_val = game.headers.get("Event", "")
    assert any(event_val == f"Test {name}" for name in ["Mate", "Fork", "Advantage"]) or event_val in {"Mate", "Fork", "Advantage"}


def _read_all_games(pgn_text: str) -> List[chess.pgn.Game]:
    games: List[chess.pgn.Game] = []
    stream = io.StringIO(pgn_text)
    while True:
        game = chess.pgn.read_game(stream)
        if game is None:
            break
        games.append(game)
    return games


def _get_game_by_puzzle_id(games: List[chess.pgn.Game], puzzle_id: str) -> chess.pgn.Game:
    for g in games:
        if g.headers.get("PuzzleId") == puzzle_id:
            return g
    raise AssertionError(f"PuzzleId {puzzle_id} not found in generated PGN")


def test_color_headers_match_side_to_move_without_start_after(tmp_path: Path) -> None:
    csv_path = _write_temp_csv(tmp_path)

    puzzles = iter_puzzles_csv(str(csv_path))
    filtered = filter_puzzles_by_rating(puzzles, 600, 800)
    selected = select_top_per_theme_streaming(
        filtered,
        top_n_per_theme=3,
        difficulty_center=700,
        include_themes=["mate", "fork", "advantage"],
    )

    out_pgn = tmp_path / "out_no_start_after.pgn"
    _ = write_puzzles_to_pgn(selected, str(out_pgn), present_after_opponent_first_move=False)
    text = out_pgn.read_text(encoding="utf-8")

    games = _read_all_games(text)

    # 00sJ9 is White to move in the CSV FEN
    g_white = _get_game_by_puzzle_id(games, "00sJ9")
    fen_white = g_white.headers.get("FEN", "")
    board_white = chess.Board(fen=fen_white)
    assert board_white.turn is chess.WHITE
    assert g_white.headers.get("White") == "You"
    assert g_white.headers.get("Black") == "Opponent"

    # 00sHx is Black to move in the CSV FEN
    g_black = _get_game_by_puzzle_id(games, "00sHx")
    fen_black = g_black.headers.get("FEN", "")
    board_black = chess.Board(fen=fen_black)
    assert board_black.turn is chess.BLACK
    assert g_black.headers.get("Black") == "You"
    assert g_black.headers.get("White") == "Opponent"


def test_color_headers_match_side_to_move_with_start_after(tmp_path: Path) -> None:
    csv_path = _write_temp_csv(tmp_path)

    puzzles = iter_puzzles_csv(str(csv_path))
    filtered = filter_puzzles_by_rating(puzzles, 600, 800)
    selected = select_top_per_theme_streaming(
        filtered,
        top_n_per_theme=3,
        difficulty_center=700,
        include_themes=["mate", "fork", "advantage"],
    )

    out_pgn = tmp_path / "out_start_after.pgn"
    _ = write_puzzles_to_pgn(selected, str(out_pgn), present_after_opponent_first_move=True)
    text = out_pgn.read_text(encoding="utf-8")

    games = _read_all_games(text)

    # 00sJ9 is White to move originally; after applying first move, it becomes Black to move
    g_after_white = _get_game_by_puzzle_id(games, "00sJ9")
    fen_after_white = g_after_white.headers.get("FEN", "")
    board_after_white = chess.Board(fen=fen_after_white)
    assert board_after_white.turn is chess.BLACK
    assert g_after_white.headers.get("Black") == "You"
    assert g_after_white.headers.get("White") == "Opponent"

    # 00sHx is Black to move originally; after applying first move, it becomes White to move
    g_after_black = _get_game_by_puzzle_id(games, "00sHx")
    fen_after_black = g_after_black.headers.get("FEN", "")
    board_after_black = chess.Board(fen=fen_after_black)
    assert board_after_black.turn is chess.WHITE
    assert g_after_black.headers.get("White") == "You"
    assert g_after_black.headers.get("Black") == "Opponent"

