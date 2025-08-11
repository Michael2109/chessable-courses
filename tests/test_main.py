from app.main import parse_args


def test_parse_args_defaults() -> None:
    args = parse_args([])
    assert args.min_rating == 600
    assert args.max_rating == 800


