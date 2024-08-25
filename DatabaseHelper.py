from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent, LoggingEventHandler
from configparser import ConfigParser
import logging, os, re, time
import sqlite3
from FFmpegUtil import FFmpegUtil
import functools


def retry_on_database_locked(retries=3, delay=1):
    """
    装饰器，用于处理数据库操作时，数据库被锁的情况。
    如果数据库被锁，则重试指定的次数，每次重试间隔delay秒。
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e):
                        attempt += 1
                        if attempt < retries:
                            time.sleep(delay)  # 等待一段时间后重试
                        else:
                            raise  # 超过重试次数后抛出异常
                    else:
                        raise  # 其他类型的 OperationalError 直接抛出
        return wrapper
    return decorator


class MyDB:
    def __init__(self, db_path, print_to_area):
        self.db_path = db_path
        self.print_area = print_to_area
        self.conn_list = []     # 保存初始化过的链接

    def get_conn(self):
        conn = sqlite3.connect(self.db_path)
        self.conn_list.append(conn)
        return conn

    def __del__(self):
        for index, cc in enumerate(self.conn_list):
            try:
                cc.close()
            except:
                self.print_area(f'close connection {index}, error')
            else:
                self.print_area(f'close connection {index}, success')

    def init_db(self):
        self.print_area(f"start init sqlite db: {self.db_path}", color="green")
        conn = self.get_conn()
        cursor = conn.cursor()
        # 需要保存啥？
        # 1. Run_Task_Record 运行过的 任务列表 + 运行结果
        # 2. Video_File_State 文件名称 + video 信息， 用于补充 run task 的输入文件的信息·
        # 3. ByPass_File_Log  用于记录 执行中 因为各种原因 不执行的 task

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "Video_File_State" (
                id INTEGER PRIMARY KEY autoincrement,         -- '主键',
                log_time INTEGER,
                file_name TEXT,                   -- '文件名称',
                file_size INTEGER,                -- '文件大小',
                file_path TEXT,                   -- '文件全路径',
                file_sample_sha256 TEXT,          -- '文件sha256简单版本！',
                video_duration INTEGER,
                video_encoder TEXT,
                video_codec TEXT,
                video_width INTEGER,
                video_height INTEGER, 
                video_pix_fmt TEXT,
                video_bit_rate INTEGER,
                video_fps INTEGER,
                audio_codec TEXT,
                audio_sample_rate INTEGER,
                audio_bit_rate INTEGER
            )
        ''')

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS "Run_Task_Record" (
                id INTEGER PRIMARY KEY autoincrement,         -- '主键',
                video_file_id INTEGER,   -- '外键  对应的vidoe文件的id
                output_video_file_id INTEGER,
                cmd TEXT,                   -- ' 命令内容
                start_running_time INTEGER, -- ' 开始运行时间
                end_running_time INTEGER,   --   结束运行时间
                cmd_output_file_path TEXT,     -- 运行输出文件 的路径
                output_has_error BOOLEAN       -- 运行输出文件中 是否有错误
            )
        ''')

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS "ByPass_File_Log" (
                id INTEGER PRIMARY KEY autoincrement, -- '主键',
                file_name TEXT,                   -- '文件名称',
                file_path TEXT,                   -- '文件全路径',
                file_size INTEGER,                -- '文件大小',
                file_sample_sha256 TEXT,          -- '文件sha256简单版本！',
                create_task_time INTEGER,           -- log时间
                last_video_file_id INTEGER,            -- 外键 video_file_state 的id
                pass_reason TEXT                     -- 略过文件理由
            )
        ''')

        # 提交事务
        conn.commit()
        logging.info(f'db {self.db_path} init done..')
        conn.close()

    @staticmethod
    def insert_ByPass_File_Log(conn, task, vfile_id, pass_reason):
        cursor = conn.cursor()
        cursor.execute(f'''
            insert into "ByPass_File_Log" (
                file_name,                
                file_size,                
                file_path,                
                file_sample_sha256,       
                create_task_time,
                last_video_file_id,
                pass_reason
            ) values (
                ?,?,?,?,?,?,?
            )
        ''', (os.path.basename(task['file_path']), task['v_info']['size'],
              task['file_path'], task['sha256'], int(time.time()), vfile_id, pass_reason))
        conn.commit()

    def check_same_sha256(self, conn, sha256_str):
        cursor = conn.cursor()
        cursor.execute('''
            select vv.id,vv.file_name, rr.output_has_error from "Video_File_State" vv
            left join "Run_Task_Record" rr on vv.id=rr.video_file_id
            where  vv.file_sample_sha256=?
        ''', (sha256_str,))

        rows = cursor.fetchall()
        return rows

    def check_success_sha256(self, conn, sha256_str):
        # 返回执行成功的 sha256 相同  的记录
        cursor = conn.cursor()
        cursor.execute('''
            select vv.id, vv.file_name,rr.id from "Video_File_State" vv
            left join "Run_Task_Record" rr on vv.id=rr.video_file_id
            where  vv.file_sample_sha256=?
                and rr.output_has_error=False
        ''', (sha256_str,))

        rows = cursor.fetchall()
        if len(rows) > 0:
            return rows[0]
        else:
            return [None,None,None ]

    @retry_on_database_locked()
    def insert_video_file_state(self, conn, info_and_sha256):
        """
        1. 每次运行task时  会记录运行的cmd 就会附带上 运行的文件信息 file state
        2. 运行结束时 会把输出的video 信息再记录一遍
        :param conn:
        :param task:
        :return:
        """
        state = info_and_sha256['v_info']
        sha256_str = info_and_sha256['sha256']
        cursor = conn.cursor()
        cursor.execute(f'''
            insert into "Video_File_State" (
                log_time,
                file_name,                   -- '文件名称',
                file_size,                -- '文件大小',
                file_path,                   -- '文件全路径',
                file_sample_sha256,          -- '文件sha256简单版本！',
                video_duration,
                video_encoder,
                video_codec,
                video_width,
                video_height, 
                video_pix_fmt,
                video_bit_rate,
                video_fps,
                audio_codec,
                audio_sample_rate,
                audio_bit_rate    
            ) values (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        ''', (int(time.time()), os.path.basename(state['file_path']), state['size'], state['file_path'],
              sha256_str, state['duration'], state['encoder'], state['video_codec'],
              state['video_width'], state['video_height'], state['video_pix_fmt'], state['video_bit_rate'],
              state['video_fps'], state['audio_codec'], state['audio_sample_rate'],state['audio_bit_rate'] )
        )
        conn.commit()
        video_file_id = cursor.lastrowid
        return video_file_id

    def record_start_run(self, conn,video_file_id, cmd, running_output_path):
        """
        记录 视频文件 开始运行cmd
        """
        cursor = conn.cursor()
        cursor.execute('''
            insert into "Run_Task_Record" (
                video_file_id,   
                cmd,                   
                start_running_time, 
                end_running_time,   
                cmd_output_file_path,  
                output_has_error    
            ) values (?,?,?,?,?,?)
        ''', (video_file_id, cmd, int(time.time()),None,running_output_path,None ))
        conn.commit()
        return cursor.lastrowid

    def record_end_run(self, conn, run_record_id, has_error, out_vfile_id):
        """
        记录 视频文件 的cmd 运行结果
        """
        cursor = conn.cursor()
        cursor.execute('''
            update "Run_Task_Record"  
            set
                output_has_error =?,
                end_running_time = ?,
                output_video_file_id = ?
            where  id=? 
        ''', (has_error, int(time.time()), out_vfile_id, run_record_id ))
        conn.commit()



def my_print(*args, color='black'):
    print(*args)

if __name__ == '__main__':

    db = MyDB("./test.db", my_print)
    db.init_db()
    conn = db.get_conn()
    ret = db.pick_file_notexists_in_db(conn,[r'D:\GGBoyProgram\NSFW\[ThZu.Cc]MDSR-0004-2.mp4'])
    print(ret)
    # rr = filter_not_decrypt_file(srcDir, desDir)
    # print(rr)
