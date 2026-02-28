from collections import deque

def read_last_lines_efficient(filename: str, n: int = 200) -> str:
    try:
        with open(filename, "r") as f:
            # deque with maxlen automatically keeps only last n lines
            last_lines = deque(f, maxlen=n)
            return "".join(last_lines)
    except Exception as e:
        return f"Error reading log file: {e}"