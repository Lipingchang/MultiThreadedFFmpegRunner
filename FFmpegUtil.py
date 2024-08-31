import subprocess
import os
import json
import queue
import traceback
import pandas
import re
import hashlib
import time

class FFmpegUtil:
    def __init__(self):
        pass

    @staticmethod
    def cal_sample_sha256(file_path):
        # 快速的计算一个大文件的sha256 值。
        # 注意！不是正确的 全文件的sha256
        # 根据文件的首尾和中间的若干块计算一个近似的SHA256值。
        # 当文件块数小于 3*每次取样个数 时候，会重复计算！

        sha256 = hashlib.sha256()
        file_size = os.path.getsize(file_path)
        block_size = 64 * 8 # hash每次处理的块大小是512字节 但看网上有把65535字节放入update中的
        samples_count = 300 # 在每个取样点 读取count个block
        block_num = file_size // block_size     # 文件包含了block_num个block
        # 三个取样点 开始的block的下标
        start_block = 0
        end_block = block_num -samples_count
        middle_block = block_num // 2
        with open(file_path, 'rb') as file:
            file.seek(start_block*block_size, 0)
            for _ in range(samples_count):
                block = file.read(block_size)
                sha256.update(block)
            file.seek(middle_block*block_size, 0)
            for _ in range(samples_count):
                block = file.read(block_size)
                sha256.update(block)
            file.seek(end_block*block_size, 0)
            for _ in range(samples_count):
                block = file.read(block_size)
                sha256.update(block)

        return sha256.hexdigest()




    @staticmethod
    def ffmpeg_video_info(input_file_path, debug=False):
        command = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format', '-show_streams',
            '-i', input_file_path
        ]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                                   encoding='utf8')
        [stdout, stderr] = process.communicate()

        jsonobj = json.loads(stdout)
        if debug:
            print(stdout)
            print(stderr)
        process.wait()

        video_stream = filter(lambda x: x['codec_type'] == 'video', jsonobj['streams'])
        audio_stream = filter(lambda x: x['codec_type'] == 'audio', jsonobj['streams'])
        video_stream = list(video_stream)
        audio_stream = list(audio_stream)
        if len(video_stream) == 0:
            raise ValueError("该文件没有包含视频流")
        if len(audio_stream) == 0:
            raise ValueError("该文件没有包含音频流")
        video_stream = video_stream[0]
        audio_stream = audio_stream[0]
        [a, b] = video_stream['avg_frame_rate'].split('/')
        fps = int(a) // int(b)
        audio_bitrate = int(audio_stream.get('bit_rate',0))
        return {
            "file_path": input_file_path,
            "duration": jsonobj['format']['duration'],
            "size": jsonobj['format']['size'],
            "encoder": jsonobj['format'].get('tags', {'encoder': 'no tag found'}).get('encoder', None),
            "video_codec": video_stream['codec_name'],
            "video_width": video_stream['width'],
            "video_height": video_stream['height'],
            "video_pix_fmt": video_stream['pix_fmt'],
            "video_bit_rate": video_stream.get(
                'bit_rate',int(jsonobj['format'].get('bit_rate',0))- audio_bitrate),
            "video_fps": fps,
            "audio_codec": audio_stream['codec_name'],
            "audio_sample_rate": audio_stream['sample_rate'],
            "audio_bit_rate": audio_bitrate
        }

    @staticmethod
    def ffmpeg_video_info_dir(dir_path):
        suffix_list = ['mp4', 'mkv', 'wmv']
        info_list = []
        for ff in os.listdir(dir_path):
            if ff.lower().split('.')[-1] in suffix_list:
                info_list.append(FFmpegUtil.ffmpeg_video_info(os.path.join(dir_path, ff)))
        df = pandas.DataFrame(info_list)
        df.to_excel("ffmpeg_info.xlsx", index=False)

    @staticmethod
    def filepath_to_av1(file_path, output_dir, global_quality):
        filename = os.path.basename(file_path)
        dir_path = os.path.dirname(file_path)
        filename = filename.split('.')[:-1]
        filename.extend([f'(qsvav1_gq{global_quality})', '.mp4'])
        filename = ''.join(filename)
        dstfile_path = os.path.join(output_dir, filename)
        # print(f"{file_path}->{dstfile_path}")
        return dstfile_path

    @staticmethod
    def ttime2second(time_str):  # time_str = '01:14:59.29'
        result = re.match(r"(\d+):(\d+):(\d+)\.\d+", time_str)
        if result:
            [hh, mm, ss] = [int(result.group(x)) for x in range(1, 4)]
            ttime_sec = hh * 3600 + mm * 60 + ss
            return ttime_sec
        else:
            # print(time_str)
            return 0

    @staticmethod
    def ffmpeg_video_to_av1_task_queue_init(file_path_list, output_dir, global_quality, running_output_dir,print_to_area):
        # file_path_list = [x['file_path'] for x in file_path_sha256_list]
        # file_sha256_list = [x['sha256'] for x in file_path_sha256_list]

        # 初始化ffmpeg任务队列
        task_queue = queue.Queue()  # 任务队列
        for i, file_path in enumerate(file_path_list):
            print_to_area(f"loading:{file_path}")
            v_info = FFmpegUtil.ffmpeg_video_info(file_path)  # 视频文件信息 列表
            dstfile_path = FFmpegUtil.filepath_to_av1(file_path, output_dir, global_quality) # 转码后 输出视频文件的路径 列表
            cmd = [
                'ffmpeg',
                '-hide_banner',
                '-i', file_path_list[i],
                '-c:v', 'hevc_qsv', '-preset', 'fast', '-global_quality', str(global_quality),
                '-look_ahead', '1', '-c:a', 'copy',
                dstfile_path, '-y'
            ]
            task_queue.put({
                'file_path': file_path,
                'duration': v_info['duration'], # 视频文件时长 列表
                'dstfile_path':  dstfile_path,
                'command': cmd,
                'v_info': v_info,
                'sha256': FFmpegUtil.cal_sample_sha256(file_path),
                'running_output_path': os.path.join(running_output_dir, f"{os.path.basename(file_path)}_{time.time()}.txt"),
            })
            print_to_area(f"👌 加入任务队列:{file_path}")

        return task_queue

    @staticmethod
    def match_ffmpeg_running_output(last_line):
        # last_line = "frame= 1077 fps= 86 q=-0.0 Lsize=    1978KiB time=00:00:35.80 bitrate= 452.7kbits/s dup=0 drop=2 speed=2.86x"
        pattern_list = [
            r'frame=\s*(\d+)',
            r'fps=\s*(\d+)',
            r'size=\s*(\d+)',
            r'time=\s*([\d:\.]+)',
            r'bitrate=\s*([\d\.]+)',
            r'speed=\s*([\d\.]+)x',
        ]
        result_list = []
        for pattern in pattern_list:
            result = re.search(pattern, last_line)
            if result is not None:
                result_list.append(result.group(1))
            else:
                return None
        return result_list

    @staticmethod
    def load_video_from_dir(dir_path):
        video_extensions = ['mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'mpg', 'ts']
        video_files = []
        for file_path in os.listdir(dir_path):
            base_n = os.path.basename(file_path)
            if base_n.split(".")[-1].lower() in video_extensions:
                video_files.append(file_path)

        video_files = [os.path.join(dir_path, x) for x in video_files]
        return video_files




