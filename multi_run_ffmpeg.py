import configparser
import shutil
import ffmpeg
import re
import subprocess
import sys
import time
import pandas as pd
import os
import json
from datetime import datetime
from collections import deque
from tqdm import tqdm
import queue
import threading
import traceback
import pandas
from FFmpegUtil import  FFmpegUtil
from TerminalOutput import TerminalOutput


class FFmpegManager(TerminalOutput, FFmpegUtil):
    custom_bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining} {postfix}]'
    def __init__(self, max_processes=1, print_buff_size=22):
        self.max_processes = max_processes
        self.print_buff_size = print_buff_size
        TerminalOutput.check_terminal_size(120, print_buff_size+max_processes+5)
        self.myprint_buff = deque(maxlen=print_buff_size)
        self.bar_start_line = 2     # 进度条输出的行号
        self.print_start_line = 3 + max_processes # print输出的行号
        self.init_output_area()
        # 同一时间只能有一个线程 向终端中输出（调用print）
        # 要同步三个：主线程会print  tqdm会print  子线程会print
        self.print_lock = threading.Lock()

    def init_output_area(self):
        # 初始化终端界面，输出有颜色的区域分割
        TerminalOutput.clear_screen()
        TerminalOutput.move_cursor(self.bar_start_line-1, 1)
        TerminalOutput.print_title("Process Bar Area:")
        TerminalOutput.move_cursor(self.print_start_line-1, 1)
        TerminalOutput.print_title("Print Output Area:")
        for i in range(self.print_buff_size):
            TerminalOutput.move_cursor(self.print_start_line+i, 1)
            sys.stdout.write(f"[{i + 1}]:")
        TerminalOutput.move_cursor(self.bar_start_line,1)


    def print_to_area(self, *args, color='black'):
        with self.print_lock:
            # 把args输出到 print_start_line 开始的区域, 字符数目超过一行的裁剪字符
            # 只显示历史 print_buff_size 条数据 多余的条目抛弃
            w = os.get_terminal_size().columns
            # 合并args参数为一个字符串 超出行的字符串裁剪去 并补上两个点
            str_args = [str(aa) for aa in args]
            print_text = " ".join(str_args).replace('\n', '\\n')
            if len(print_text) > w - 5:
                print_text = print_text[:w - 7] + '..'
            if len(self.myprint_buff) >= self.print_buff_size:
                self.myprint_buff.popleft()
            self.myprint_buff.append({'t':print_text,'c':color})

            # 把缓存的print内容输出到 终端的指定区域
            ll = min(len(self.myprint_buff), self.print_buff_size)
            for i in range(ll):
                self.move_cursor(self.print_start_line + i, 1)
                sys.stdout.write(" " * (w-1))  # Clear the line
                self.move_cursor(self.print_start_line + i, 1)
                c = self.myprint_buff[i]['c']
                if c == 'red':
                    sys.stdout.write("\033[31m")
                sys.stdout.write(f"[{i + 1}]: {self.myprint_buff[i]['t']}\n")
                sys.stdout.write('\033[0m')

            self.move_cursor(self.bar_start_line, 1)
            sys.stdout.flush()


    @staticmethod
    def enqueue_output(out, q, print_to_area, thread_name):
        mypid = os.getpid()
        t_name = threading.current_thread().name
        print_to_area(f'start thread {t_name},{thread_name}, pid:{mypid}')
        for line in out:
            q.append(line)
        out.close()
        print_to_area(f'start thread {t_name},{thread_name}, pid:{mypid}')

    def run(self, file_path_list, output_dir,global_quality=24):
        ready_task_queue = FFmpegUtil.ffmpeg_video_to_av1_task_queue_init(
            file_path_list, output_dir, global_quality)
        running_process_list = [None] * self.max_processes
        done_process_list = []

        with self.print_lock:
            for i in range(self.max_processes):  # 初始化进度条 和 进程信息
                running_process_list[i] = {
                    "process": None,
                    "output": None,
                    "pbar": tqdm(total=100, bar_format=self.custom_bar_format, position=i),
                    "task": None,
                }

        try:
            while True:
                # 1. 检查running里面有没有 None, 安排ready task 进入
                for i, process_info in enumerate(running_process_list):
                    if process_info["process"] is None:
                        try:
                            task = ready_task_queue.get(block=False)
                            process = subprocess.Popen(
                                task['command'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, encoding='utf8',
                                shell=False)  # shell = True 时运行 process是shell进程 ffmpeg是shell的子进程 kill时 会让ffmpeg继续运行
                            output_queue = deque(maxlen=10)  # 创建输出队列和线程
                            t = threading.Thread(
                                target=self.enqueue_output,
                                args=(process.stderr, output_queue,
                                      self.print_to_area,os.path.basename(task["file_path"]))
                            )
                            t.daemon = True  # 设置为守护线程，使其在主线程结束时自动退出
                            t.start()
                            with self.print_lock:
                                running_process_list[i]["pbar"].total = task['duration']  # 设置进度条最大长度
                                running_process_list[i]["pbar"].n = 0
                            running_process_list[i]["process"] = process
                            running_process_list[i]["output"] = output_queue
                            running_process_list[i]["task"] = task
                        except queue.Empty:
                            running_process_list[i]['process'] = None

                # 2. 从进度队列中取出进度 显示到进度条上
                for i, process_info in enumerate(running_process_list):
                    pbar = process_info['pbar']
                    if process_info["process"] is None:
                        continue
                    output_queue = process_info['output']
                    pbar.set_description_str(os.path.basename(process_info['task']['file_path']))
                    try:
                        last_line = output_queue.pop()
                        result = FFmpegUtil.match_ffmpeg_running_output(last_line)
                        if result is not None:
                            [frame, fps, size, ttime, bitrate, speed] = result
                            tt = FFmpegUtil.ttime2second(ttime)
                            pbar.n = tt
                            pbar.set_postfix_str(f"bitrate:{bitrate},speed:{speed}")
                        elif "error" in last_line.lower():
                            self.print_to_area(f'error in output:[{last_line}]', color='red')
                        else:
                            pass
                    except IndexError:
                        pass  # 没有从队列中获取到消息

                # 3. 刷新进度条
                with self.print_lock:
                    for i, process_info in enumerate(running_process_list):
                        pbar = process_info['pbar']
                        pbar.refresh()

                # 4. 检查进程状态, 运行完毕的 放入done中 把running置 None
                for i, process_info in enumerate(running_process_list):
                    if process_info['process'] is None:
                        continue
                    retcode = process_info['process'].poll()
                    if retcode is not None:
                        self.print_to_area(f"{process_info['task']['file_path']} is exited, ret code:{retcode}")
                        done_process_list.append({
                            "process": process_info["process"],
                            "output": process_info["output"],
                            "task": process_info["task"]
                        })
                        running_process_list[i]['process'] = None

                # 5. readytask为空 且 running为空 就退出循环
                running_c =  len(list(filter(lambda x: x['process'] is not None, running_process_list))) # print_to_area(f"running_c:{running_c}")
                if ready_task_queue.empty() and running_c == 0:
                    break
                time.sleep(0.1)
        except KeyboardInterrupt as ke:
            self.print_to_area("用户键盘退出")
            # 还需要杀掉一些进程
            for p in running_process_list:
                if p['process'] is not None:
                    p['process'].kill()
        except Exception as e:
            self.print_to_area("其他类型的报错。。")
            self.print_to_area(f"Error type: {type(e).__name__}")
            self.print_to_area(f"Error message: {e}")
            self.print_to_area("Stack trace:")
            traceback.print_exc()

        # 进度条关闭
        for p in running_process_list:
            p['pbar'].close()


def read_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config



if __name__ == "__main__":
    config = read_config(os.path.join(".", "config.ini"))
    max_processes = config.getint("Input","max_processes")
    print_buff_size = config.getint("Input","print_buff_size")
    video_dir_path = config.get("Input","video_dir_path")
    video_out_path = config.get("Output","video_dir_path")
    quality = config.getint("Output", "global_quality")


    if not os.path.exists(video_out_path):
        res = os.makedirs(video_out_path, exist_ok=False) # 父目录不存在会报错

    video_file_list = FFmpegUtil.load_video_from_dir(video_dir_path)
    manager = FFmpegManager(max_processes=5, print_buff_size=10)

    manager.run( video_file_list, video_out_path, quality)
    TerminalOutput.move_cursor(
        manager.max_processes + manager.print_buff_size + 5,
        1
    )
