from datetime import datetime


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def event_display_name(event, fallback_user_id: str | None = None) -> str:
    user_id = str(fallback_user_id if fallback_user_id is not None else event.get_user_id())
    try:
        sender = getattr(event, "sender", None)
        if sender:
            return sender.card or sender.nickname or user_id
    except Exception:
        pass
    return user_id


def parse_board_coord(text: str, width: int | None = None, height: int | None = None):
    coord = str(text or "").strip().upper()
    if len(coord) < 2:
        return None

    letters = ""
    nums = ""
    for ch in coord:
        if ch.isalpha():
            letters += ch
        elif ch.isdigit():
            nums += ch

    if not letters or not nums:
        return None

    col = 0
    for i, ch in enumerate(reversed(letters)):
        col += (ord(ch) - ord("A") + 1) * (26 ** i)
    col -= 1
    row = int(nums) - 1

    if col < 0 or row < 0:
        return None
    if width is not None and col >= width:
        return None
    if height is not None and row >= height:
        return None
    return col, row


def format_board_coord(x: int, y: int) -> str:
    n = int(x) + 1
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord("A") + (n % 26)) + letters
        n //= 26
    return f"{letters}{int(y) + 1}"
