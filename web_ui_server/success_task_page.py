import streamlit as st
from sqlalchemy.sql import text
import pandas as pd
import datetime
import altair as alt

st.set_page_config(layout="wide")
conn = st.connection('video_log', type='sql')
timestamp_to_str = lambda x: datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
hour_to_str = lambda seconds: f"{int(seconds // 3600)}:{int((seconds % 3600) // 60)}:{int(seconds % 60)}"
toGB = lambda x: f"{round(int(x) / 1024 / 1024 / 1024, 2)}GB"
ratetoMB = lambda x: f"{round(int(x) / 1024 / 1024 , 2)}MB"

with (conn.session as s):

    def get_not_sha_bypass_list():
        rst = s.execute(text(f'''
            select
                bypass.file_name, bypass.pass_reason,create_task_time as 'start_running_time'
            from ByPass_File_Log bypass
            left join Video_File_State input_video on bypass.last_video_file_id=input_video.id 
            where pass_reason not like '文件sha256在库中已出现%'
        '''))
        df = pd.DataFrame(rst)
        df["start_running_time"] = df["start_running_time"].apply(timestamp_to_str)
        df['start_running_date'] = pd.to_datetime(df['start_running_time']).dt.date
        df['start_running_date_str'] = df['start_running_date'].apply(
            lambda x: x.strftime("%Y-%m-%d"))
        return df

    def get_task_list(has_error):
        '''
        查询成功运行/失败运行的task的个数
        '''
        query_str = "where record.output_has_error=:output_has_error"
        if has_error is None:
            query_str = "where record.output_has_error is Null"
        rst = s.execute(text(f'''
            select 
                record.id, input_video.file_name, record.start_running_time, record.end_running_time,
                input_video.file_size as "input_size",output_video.file_size as "out_size",
                input_video.video_duration, input_video.video_width, input_video.video_height,
                input_video.video_bit_rate as "input_bitrate", output_video.video_bit_rate as "output_bitrate",
                input_video.video_codec as "input_codec", output_video.video_codec as "output_codec", record.id as "record_id", cmd
            from Run_task_Record record
            left join Video_File_State input_video on record.video_file_id=input_video.id 
            left join Video_File_State output_video on record.output_video_file_id=output_video.id
            {query_str}
            order by record.end_running_time desc
        '''), {
            'output_has_error': has_error
        })
        df = pd.DataFrame(rst)
        df["start_running_time"] = df["start_running_time"].apply(timestamp_to_str)
        df['start_running_date'] = pd.to_datetime(df['start_running_time']).dt.date
        df['start_running_date_str'] = df['start_running_date'].apply(
            lambda x: x.strftime("%Y-%m-%d"))
        return df

    def get_task_daily_count( has_error):
        result_df = get_task_list(has_error)
        daily_success_df = result_df.groupby(by='start_running_date_str',as_index=False).agg({'file_name':'count'})
        # st.markdown(f"在{daily_success_df.shape[0]}个日期中成功运行，成功运行task条数:{success_df.shape[0]}")
        # 把日期类型的列 转换成字符串，可以在图表上显示，不然会显示一个区间
        return daily_success_df


    fail_df = get_task_list(True).groupby(by='start_running_date_str',as_index=False).agg({'file_name':'count'}) # get_task_daily_count(True)
    fail_df['task_result'] = "fail"
    success_df = get_task_list(False).groupby(by='start_running_date_str',as_index=False).agg({'file_name':'count'})  #get_task_daily_count(False)
    success_df['task_result'] = 'success'
    bypass_df = get_not_sha_bypass_list().groupby(by='start_running_date_str', as_index=False).agg({'file_name': 'count'})
    bypass_df['task_result'] = 'bypass'
    df = pd.concat([success_df,fail_df,bypass_df])
    df = df.sort_values(by='start_running_date_str', ascending=False)   # 日期 降序

    custom_colors = ['#1f77b4', '#d62728', '#2ca02c', '#ff7f0e', '#9467bd', '#8c564b']

    bars_chart = alt.Chart(df).mark_bar().encode(
            x=alt.X('task_result:O',title=""),
            y=alt.Y('sum(file_name):Q',title="该日期运行成功次数", scale=alt.Scale(type='symlog')),
            # color='task_result:N',
            color=alt.Color('task_result:N', scale=alt.Scale(range=custom_colors)),  # 使用自定义颜色列表
            # column='start_running_date_str', # 不能在这里分列 要bar和text合并后再分列
    )
    text_chart = bars_chart.mark_text(
            align='center',
            baseline='middle',
            dy=-5,  # 小幅向上偏移
    ).encode(
        text='sum(file_name):Q',
        color='task_result:N',
    )
    bars_with_text = (bars_chart+text_chart).facet(
        column=alt.Column('start_running_date_str:N',title="开始运行日期",  sort=alt.SortOrder("descending"))
    )
    st.altair_chart(
        bars_with_text
    )

    date_option_list = df['start_running_date_str'].drop_duplicates()
    date_option = st.selectbox(
        "选择日期，查看运行结果",
        date_option_list
    )


    success_df = get_task_list(False)
    success_df = success_df[success_df['start_running_date_str'] == date_option]
    success_df_input_size_sum = success_df['input_size'].sum()
    success_df_output_size_sum = success_df['out_size'].sum()
    fail_df = get_task_list(True)
    fail_df = fail_df[fail_df['start_running_date_str'] == date_option]
    fail_input_size_sum = fail_df['input_size'].sum()
    fail_output_size_sum = fail_df['out_size'].sum()
    bypass_df = get_not_sha_bypass_list()
    bypass_df = bypass_df[bypass_df['start_running_date_str'] == date_option]

    st.markdown(f"当日，成功个数:{success_df.shape[0]}, 失败个数:{fail_df.shape[0]}, bypass个数:{bypass_df.shape[0]}")
    st.markdown(f"文件总计:{success_df.shape[0] + fail_df.shape[0] + bypass_df.shape[0]}")
    st.markdown("请手动 比较下 文件总计数量 和 输入文件夹中文件数量 ")

    success_df["compass_rate"] = success_df["input_size"] / success_df["out_size"]
    st.markdown("## 压缩率 小于 1")
    rate_lower_1 = success_df[success_df["compass_rate"] < 1]
    rate_lower_1 = rate_lower_1[['file_name','input_size','out_size','video_duration','video_width','video_height','input_bitrate','output_bitrate']]
    rate_lower_1['video_duration'] = rate_lower_1['video_duration'].apply(hour_to_str)
    rate_lower_1['input_size'] = rate_lower_1['input_size'].apply(toGB)
    rate_lower_1['out_size'] = rate_lower_1['out_size'].apply(toGB)
    rate_lower_1['input_bitrate'] = rate_lower_1['input_bitrate'].apply(ratetoMB)
    rate_lower_1['output_bitrate'] = rate_lower_1['output_bitrate'].apply(ratetoMB)
    st.dataframe(rate_lower_1, use_container_width = True)


    st.markdown(f"## success \n当天总 原文件大小: {toGB(success_df_input_size_sum)}, 生成文件大小：{toGB(success_df_output_size_sum)}")
    success_df
    st.markdown(f"## fail  \n当天总 原文件大小: {toGB(fail_input_size_sum)}, 生成文件大小：{toGB(fail_output_size_sum)}")
    fail_df
    st.markdown(f"## bypass ")
    bypass_df




