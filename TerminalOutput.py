import os
import sys
import re

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


    @staticmethod
    def get_display_width(s):
        # 计算字符串的显示宽度
        one_char_width = re.findall(r'[A-Za-z0-9\u0020-\u007F]', s)  # 英文字符、数字和常见标点
        one_char_count = len(one_char_width)
        # 其他字符默认占用 2 个字符宽度
        total_width = one_char_count + 2 * (len(s) - one_char_count)
        return total_width

    @staticmethod
    def truncate_string_by_width(s, l):
        # 如果字符串的宽度小于或等于 l，直接返回原字符串
        if TerminalOutput.get_display_width(s) <= l:
            return s

        # 否则，逐个字符检查，直到达到指定宽度
        result = []
        current_width = 0
        for char in s:
            char_width = 2 if re.match(r'[^A-Za-z0-9\u0020-\u007F]', char) else 1  # 判断字符是否为非ASCII字符
            if current_width + char_width > l-3:
                break
            result.append(char)
            current_width += char_width

        return ''.join(result) + ".."