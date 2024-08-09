import subprocess
import os
import json
import queue
import traceback
import pandas
import re

class FFmpegUtil:
    def __init__(self):
        pass

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
        return {
            "file_path": input_file_path,
            "duration": jsonobj['format']['duration'],
            "size": jsonobj['format']['size'],
            "encoder": jsonobj['format']['tags'].get('encoder', None),
            "video_codec": video_stream['codec_name'],
            "video_width": video_stream['width'],
            "video_height": video_stream['height'],
            "video_pix_fmt": video_stream['pix_fmt'],
            "video_bit_rate": video_stream['bit_rate'],
            "video_fps": fps,
            "audio_codec": audio_stream['codec_name'],
            "audio_sample_rate": audio_stream['sample_rate'],
            "audio_bit_rate": audio_stream['bit_rate']
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
    def ffmpeg_video_to_av1_task_queue_init(file_path_list, output_dir, global_quality):
        # 初始化ffmpeg任务队列
        # 每一个原视频文件生成一条转换格式的 cmd命令
        v_info_list = [
            FFmpegUtil.ffmpeg_video_info(x)
            for x in file_path_list
        ]  # 视频文件信息 列表
        duration_list = [
            int(float(v_info['duration']))
            for v_info in v_info_list
        ]  # 视频文件时长 列表
        dstfile_path_list = [
            FFmpegUtil.filepath_to_av1(file_path, output_dir, global_quality)
            for file_path in file_path_list
        ]  # 转码后 输出视频文件的路径 列表
        command_list = [[
            'ffmpeg',
            '-hide_banner',
            '-i', file_path_list[i],
            '-c:v', 'hevc_qsv', '-preset', 'fast', '-global_quality', str(global_quality),
            '-look_ahead', '1', '-c:a', 'copy',
            dstfile_path_list[i], '-y'
        ] for i in range(len(file_path_list))] # ffmepg命令列表
        task_queue = queue.Queue()  # 任务队列

        for i, file_path in enumerate(file_path_list):
            task_queue.put({
                'file_path': file_path,
                'duration': duration_list[i],
                'dstfile_path': dstfile_path_list[i],
                'command': command_list[i],
                'v_info': v_info_list[i]
            })
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
        video_extensions = ['mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm']
        video_files = []
        for file_path in os.listdir(dir_path):
            base_n = os.path.basename(file_path)
            if base_n.split(".")[-1].lower() in video_extensions:
                video_files.append(file_path)

        video_files = [os.path.join(dir_path, x) for x in video_files]
        return video_files




