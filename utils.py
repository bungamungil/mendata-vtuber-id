import column_names as cn
import label

log_top_contributors_count = 10

yt_js_initial_data_var_name = 'var ytInitialData ='

q_stats = [
    (label.log_q_by_claim, cn.q_by_claim),
    (label.log_q_by_original_character, cn.q_by_original_character),
    (label.log_q_by_content, cn.q_by_content),
    (label.log_q_by_already_debut, cn.q_by_already_debut),
    (label.log_q_by_active_on_past_3_months, cn.q_by_active_on_past_3_months),
    (label.log_q_by_public_subs_count, cn.q_by_public_subs_count),
]

map_column_names = {
    'Timestamp': cn.input_timestamp,
    'Nama/inisial Kontributor': cn.contributor_name,
    'Nama Channel Vtuber': cn.yt_channel_name,
    'Tautan Channel Youtube Vtuber': cn.yt_channel_url,
    'Tanggal Debut': cn.debut_date,
    'Kualifikasi #MendataVtuberID [Mengakui diri dan diakui oleh khalayak sebagai Virtual Youtuber Indie Indonesia]':
        cn.q_by_claim,
    'Kualifikasi #MendataVtuberID [Memiliki dan menggunakan original character]': cn.q_by_original_character,
    'Kualifikasi #MendataVtuberID [Merupakan Virtual Youtuber independen yang telah debut]': cn.q_by_already_debut,
    'Kualifikasi #MendataVtuberID [Akun Youtube didominasi oleh konten Virtual Youtuber]': cn.q_by_content,
    'Kualifikasi #MendataVtuberID [Akun Virtual Youtuber dengan keaktifan maksimal 3 bulan terakhir]':
        cn.q_by_active_on_past_3_months,
    'Kualifikasi #MendataVtuberID [Jumlah subscriber Youtube terlihat secara publik]': cn.q_by_public_subs_count,
    'Unnamed: 12': cn.note
}


def insert_channel_id_into_df(row):
    return guess_channel_id(row[cn.yt_channel_url])


def guess_channel_id(youtube_url):
    split = youtube_url.split('/')
    if len(split) < 5:
        return None
    return split[4]


def generate_statistics(df, state, s_qualifications, s_contributors):
    print('\n--- ### {} DATA'.format(state.upper()))
    print(label.log_total_rows.format(state, len(df)))
    generate_statistic_qualifications(df, state, s_qualifications)
    generate_statistic_contributors(df, state, s_contributors)


def generate_statistic_qualifications(df, state, statistic):
    statistic[state] = {}
    for (q_stat_label, q_stat_column_name) in q_stats:
        statistic[state][q_stat_column_name] = df[q_stat_column_name].value_counts()
        print(q_stat_label.format(state))
        print(statistic[state][q_stat_column_name])


def generate_statistic_contributors(df, state, statistic):
    statistic[state] = df[cn.contributor_name].value_counts()
    print(label.log_top_contributors.format(log_top_contributors_count, state))
    print(statistic[state].head(log_top_contributors_count))


def find_json_text(js_text):
    x = js_text.find(yt_js_initial_data_var_name) + len(yt_js_initial_data_var_name)
    y = js_text.find(';')
    return js_text[x:y]


def fix_time_dict(fmt, amount):
    if fmt[-1] != 's':
        fmt = '{}s'.format(fmt)
    return fmt, int(amount)