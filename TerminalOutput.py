import os
import sys


class TerminalOutput:
    # 命令行终端操作
    # 在这个类中 可以移动光标 输出带有颜色的文本

    def __init__(self):
        pass

    @staticmethod
    def clear_screen():
        sys.stdout.write("\033[2J")
        sys.stdout.flush()

    @staticmethod
    def move_cursor(x, y):  # row / column
        sys.stdout.write(f"\033[{x};{y}H")
        sys.stdout.flush()

    @staticmethod
    def print_title(text, split_text="="):
        text_c = len(text)
        terminal_width = os.get_terminal_size().columns
        if text_c + 2 >= terminal_width:
            sys.stdout.write(f"\033[31m{split_text * 3} {text} {split_text * 3}\033[0m")
        else:
            split_text_c = (terminal_width - text_c - 2) // 2
            sys.stdout.write(f"\033[31m{split_text * split_text_c} {text} {split_text * split_text_c}\033[0m")
        sys.stdout.write("\n")
        sys.stdout.flush()

    @staticmethod
    def check_terminal_size(min_w, min_h):
        w = os.get_terminal_size().columns
        h = os.get_terminal_size().lines
        if min_w <= w and min_h <= h:
            return True
        else:
            t = f"Current terminal size ({h}x{w}) does not meet requirements: {min_h}x{min_w}"
            raise ValueError(t)
