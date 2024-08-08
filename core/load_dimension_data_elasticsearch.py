import pymysql
import pandas as pd
from datetime import datetime
import json


# Load local database connetion params
conn_file = open("import\\params\\connection_params.json", "r")
conn_params = json.load(conn_file)

local_host_ = conn_params.get("local_host")
local_user_ = conn_params.get("local_user")
local_pass_ = conn_params.get("local_pass")
local_port_ = conn_params.get("local_port")


# using ra.resourceToAcademicLevelEdExDim
# using ra.academicLevelEdExDim
# using ra.MemberEdexDim m
memberEduLevel_query = """
select e.memberId, a.eduLevel, count(e.memberId) as cnt_memberEduLevel, sum(event_count) as sum_EventCount, t.layer
from els.agg_elasticsearchevents_1 e
join edx.resourceToAcademicLevel t on t.resourceId = e.resourceId
join edx.academicLevel a on a.id = t.academicLevels
join edx.Member m on m.id = e.memberId
where 1 =1 
-- and e.event like '%resource%'
and e.resourceId is not null
and e.entityType = 'resource'
and e.eventLevel is not null
and (
-- Download
e.event = 'v1.resource.fetched'
or e.event = 'resource.click.downloadToDevice'
or e.event = 'resource.click.relatedResource'
or e.event = 'resource.click.sendToGD'
or e.event = 'resource.click.weblink'
or e.event = 'v1.resource.export-to-gd-trigger'
or e.event = 'v1.resource.export-to-gd-success'
or e.event = 'resource.click.sendToOD'
or e.event = 'v1.resource.export-to-od-trigger'
or e.event = 'v1.resource.export-to-od-success'
or e.event = 'resource.click.ccxTemplateLink'
or e.event = 'resource.click.ccxTemplateLink.inline'
or e.event = 'v1.resource.export-zip-trigger'
or e.event = 'v1.resource.export-zip-success'
-- Viewed
or e.event = 'v1.engaged.resourceLink'
or e.event = 'resource.pageview.details'
or e.event = 'resource.subview.asset'
or e.event = 'resource.click.share'
or e.event = 'resource.click.inlineLink'
or e.event = 'resource.click.ccxTemplateLink'
-- Created
or e.event = 'v1.resource.updated'
or e.event = 'v1.resource.created'
or e.event = 'v1.resource.published')
group by e.memberId, a.eduLevel, t.layer;
"""



def get_es_dim_data():   
    listDimDataDataFrame_ = [] # empty list of Pandas df
    dbConnection = pymysql.connect(
        host=local_host_, user=local_user_,
        password=local_pass_, port=local_port_
    )
    print("(Elastic search) Dimension data load started: ", datetime.now())
    try:
        with dbConnection.cursor() as cursor:
            # [0] - memberEduLevel - elasticsearchevents_agg, resourceToAcademicLevelEdExDim, academicLevelEdExDim, ra.MemberEdexDim
            query = memberEduLevel_query
            cursor.execute(query)
            rows = cursor.fetchall()
            df_dimDataESSegmentation_ = pd.DataFrame(rows, columns = ["memberId", "eduLevel", "cnt_memberEduLevel", "sum_EventCount", "layer"], index = None)
            listDimDataDataFrame_.append(df_dimDataESSegmentation_)
            del rows
    finally:
        print("(Elastic search) Dimension data load finished: ", datetime.now())
        dbConnection.close()
    #tunnel.stop() # stop the tunnel
    return listDimDataDataFrame_


def get_member_edu_level():
    return listDimData[0]


listDimData = get_es_dim_data()