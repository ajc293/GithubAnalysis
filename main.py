import gzip
import json
import pandas
import requests

def get_data():
    url = f'http://data.githubarchive.org/2018-10-01-3.json.gz'
    r = requests.get(url)
    filename = url.split('/')[-1]
    with open(filename, 'wb') as f:
        f.write(r.content)
    lines = []
    for line in gzip.open(filename, 'rb'):
        lines.append(json.loads(line))
    df = pandas.DataFrame(lines)
    df = df.set_index('id')
    return df

def extend_table(data):
    data = pandas.concat([data.drop(['actor'], axis=1), data['actor'].apply(pandas.Series)], axis=1)
    data = pandas.concat([data.drop(['payload'], axis=1), data['payload'].apply(pandas.Series)], axis=1)
    data = pandas.concat([data.drop(['repo'], axis=1), data['repo'].apply(pandas.Series)], axis=1)
    data = pandas.concat([data.drop(['org'], axis=1), data['org'].apply(pandas.Series)], axis=1)
    data['index'] = data.index
    data.columns = ['created_at',
                    'public',
                    'type',
                    'actor_id',
                    'actor_login',
                    'actor_display_login',
                    'actor_gravatar_id',
                    'actor_url',
                    'actor_avatar_url',
                    'payload_action',
                    'payload_issue',
                    'payload_comment',
                    'payload_push_id',
                    'payload_size',
                    'payload_distinct_size',
                    'payload_ref',
                    'payload_head',
                    'payload_before',
                    'payload_commits',
                    'payload_forkee',
                    'payload_ref_type',
                    'payload_master_branch',
                    'payload_description',
                    'payload_pusher_type',
                    'payload_number',
                    'payload_pull_request',
                    'payload_release',
                    'payload_pages',
                    'payload_member',
                    'repo_id',
                    'repo_name',
                    'repo_url',
                    'org_id',
                    'org_login',
                    'org_gravatar_id',
                    'org_url',
                    'org_avatar_url',
                    'org_0',
                    'index']
    return data

def count_distinct_id(df, col):
    return df[[col, 'index']].groupby([col]).agg(['count'])

spreadsheet = pandas.ExcelWriter('GitHubFull.xlsx')
gh = extend_table(get_data())

count_distinct_id(gh, 'actor_id').to_excel(spreadsheet, 'distinct_actors')
count_distinct_id(gh, 'repo_id').to_excel(spreadsheet, 'distinct_repo')
count_distinct_id(gh, 'org_id').to_excel(spreadsheet, 'distinct_repo')
count_distinct_id(gh, 'type').to_excel(spreadsheet, 'distinct_types')



gh.to_excel(spreadsheet, 'data')

spreadsheet.save()

