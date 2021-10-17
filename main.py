import datetime
from dateutil import relativedelta

import pandas as pd
import os
import utils as utils
import label
import column_names as cn
from lxml import html
import requests
import json


df = pd.read_csv(os.getenv(label.IMPORT_FILE))\
    .rename(columns=utils.map_column_names, errors='raise')\
    .drop(columns=['Media Sosial'])

# Proceed raw data
utils.generate_statistics(df, 'raw')

# Proceed clean data
df.loc[:, cn.yt_channel_id] = df.apply(lambda _row: utils.insert_channel_id_into_df(_row), axis=1)
df = df\
    .dropna(subset=[cn.yt_channel_id])\
    .drop_duplicates(subset=[cn.yt_channel_id], keep='last')\
    .drop(columns=[cn.yt_channel_url])
utils.generate_statistics(df, 'clean')


# Proceed valid data
sample_df = df.head(20)
for row in sample_df.itertuples(name='Row'):
    page = requests.get('https://www.youtube.com/channel/{}/videos?hl=en'.format(row.yt_channel_id))
    tree = html.fromstring(page.content)
    js_text = tree.xpath("//script[contains(., 'ytInitialData')]/text()")[0]
    data = json.loads(utils.find_json_text(js_text))
    yt_tabbed_header = data['header']['c4TabbedHeaderRenderer']
    if 'title' in yt_tabbed_header:
        sample_df.loc[row.Index, cn.yt_current_channel_name] = yt_tabbed_header['title']
    else:
        if 'alerts' in data:
            sample_df.loc[row.Index, cn.auto_verify_q_by_active_on_past_3_months] = 'Tidak'
            sample_df.loc[row.Index, cn.yt_alert_text] = data['alerts'][0]['alertRenderer']['text']['simpleText']
            continue
    if 'avatar' in yt_tabbed_header:
        sample_df.loc[row.Index, cn.yt_current_avatar] = yt_tabbed_header['avatar']['thumbnails'][-1]['url']
    if 'banner' in yt_tabbed_header:
        sample_df.loc[row.Index, cn.yt_current_banner] = yt_tabbed_header['banner']['thumbnails'][-1]['url']
    if 'subscriberCountText' in yt_tabbed_header:
        sample_df.loc[row.Index, cn.auto_verify_q_by_public_subs_count] = 'Ya'
        subs_count = data['header']['c4TabbedHeaderRenderer']['subscriberCountText']['simpleText'].split(' ')[0]
        sample_df.loc[row.Index, cn.yt_subscribers_count] = subs_count
    else:
        sample_df.loc[row.Index, cn.auto_verify_q_by_public_subs_count] = 'Tidak'
    videos_tab_content = last_video = data['contents']['twoColumnBrowseResultsRenderer']['tabs'][1]['tabRenderer']['content']['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0]
    if 'gridRenderer' in videos_tab_content:
        last_video = videos_tab_content['gridRenderer']['items'][0]['gridVideoRenderer']
        sample_df.loc[row.Index, cn.yt_last_video_id] = last_video['videoId']
        sample_df.loc[row.Index, cn.yt_last_video_thumbnail] = last_video['thumbnail']['thumbnails'][-1]['url']
        sample_df.loc[row.Index, cn.yt_last_video_title] = last_video['title']['runs'][0]['text']
        split_published_time_text_info = last_video['publishedTimeText']['simpleText'].split()
        if split_published_time_text_info[0] == 'Streamed':
            sample_df.loc[row.Index, cn.yt_last_video_type] = 'Streaming'
        else:
            sample_df.loc[row.Index, cn.yt_last_video_type] = 'Video'
        time_string = [split_published_time_text_info[-3:-1]]
        time_dict = dict(utils.fix_time_dict(fmt, amount) for amount, fmt in time_string)
        dt = relativedelta.relativedelta(**time_dict)
        video_published_time = datetime.datetime.now() - dt
        sample_df.loc[row.Index, cn.yt_last_video_published_time] = '{} ago'.format(' '.join(time_string[0]))
        if video_published_time < datetime.datetime.now() - relativedelta.relativedelta(months=3):
            sample_df.loc[row.Index, cn.auto_verify_q_by_active_on_past_3_months] = 'Tidak'
        else:
            sample_df.loc[row.Index, cn.auto_verify_q_by_active_on_past_3_months] = 'Ya'
        sample_df.loc[row.Index, cn.yt_last_video_view_count] = last_video['viewCountText']['simpleText'].split()[0]
        sample_df.loc[row.Index, cn.yt_last_video_duration] = last_video['thumbnailOverlays'][0]['thumbnailOverlayTimeStatusRenderer']['text']['simpleText']
    else:
        sample_df.loc[row.Index, cn.auto_verify_q_by_active_on_past_3_months] = 'Tidak'
        if 'messageRenderer' in videos_tab_content:
            sample_df.loc[row.Index, cn.yt_alert_text] = videos_tab_content['messageRenderer']['text']['simpleText']
            continue

sample_df.loc[:, cn.yt_subscribers_count] = sample_df[cn.yt_subscribers_count].fillna(0).replace({'K': '*1e3', 'M': '*1e6'}, regex=True).map(pd.eval).astype(int)

sample_verified_df = sample_df[sample_df[cn.auto_verify_q_by_active_on_past_3_months] == 'Ya'][sample_df[cn.auto_verify_q_by_public_subs_count] == 'Ya']
print(len(sample_df))

verified_column_titles = [
    '#',
    'Channel Name',
    'Channel Avatar',
    'Subscriber Count',
    'Last Video Thumbnail',
    'Last Video Title',
]
with open('verified.html', 'w', encoding='utf-8') as f:
    f.write('<html>')
    f.write('<head>')
    f.write('<style>')
    f.write("table {"
            "  font-family: arial, sans-serif;"
            "  border-collapse: collapse;"
            "  width: 100%;"
            "}"
            "td, th {"
            "  border: 1px solid #dddddd;"
            "  text-align: center;"
            "  padding: 8px;"
            "}"
            "tr:nth-child(even) {"
            "  background-color: #dddddd;"
            "}")
    f.write('</style>')
    f.write('</head>')
    f.write('<body>')
    f.write('<table>')
    f.write('<tr>')
    for title in verified_column_titles:
        f.write('<th>{}</th>'.format(title))
    f.write('</tr>')
    for row in sample_verified_df.itertuples(name='Row'):
        f.write('<tr>')
        f.write('<td>{}</td>'.format(row.Index))
        f.write('<td style="text-align: left;">{}</td>'.format(row.yt_current_channel_name))
        f.write('<td><img src={}></td>'.format(row.yt_current_avatar))
        f.write('<td>{}</td>'.format(row.yt_subscribers_count))
        f.write('<td><img src={}></td>'.format(row.yt_last_video_thumbnail))
        f.write('<td><a href="https://youtu.be/{}" target="_blank">{}</a></td>'.format(row.yt_last_video_id, row.yt_last_video_title))
        f.write('</tr>')
    f.write('</table></body></html>')
