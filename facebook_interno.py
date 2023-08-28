# Importa librerías
import  pandas as pd
import requests
from datetime import datetime
import time
import os
import boto3
import botocore
import sys
from io import StringIO
from tqdm import tqdm
from datetime import datetime, timedelta
import facebook #facebook-sdk
from random import randint
from tqdm.notebook import tqdm, trange

"""
25/08/2023 SAG LINEA 207: Consulta de seguidores para las marcas

"""


today = datetime.now()
current_day1 = today.strftime("%d")
current_year1 = today.strftime("%Y")
current_month1 = today.strftime("%m")


api_key = "EAABuf8tzBA0BAHJwwlaM5RfWBZBVMLGg1KFhKvkiis9JgyMAgAkLCtitZAMsUx93BX0g3lXXuX16PInooSjfJ0cgXX3I3kRGLhO6xZBrTUNRGvStIrfAkjvtfaQQzH6e1kNsP5SvRTlZCv7ceZAL20Esdtcaue3EHQia6FTWEWvAEXVkjMRNG"
# tokens de los portales que tienen info de Facebook (mantener fijo)
dict_pages= {
               #"desafiothebox" : "130529137121205",
               "CaracolSports": "103357148630572",
               "CaracolTelevisión":"150579504984669", ##
               "NoticiasCaracol": "216740968376511",##
               "Shock": "51955958533", 
               "Volk": "114261129958524",
               "LaKalle": "312295662491632",
               "BluRadio": "128385783971639",## 
               "GolCaracol": "123368131081367", ##  
               "HJCK" : "275031519209396",
               "TeatroMayorJulioMarioSantoDomingo" : "189844404462391",
               "LaDescargaReality" : "104271092421342"
               
}
def url_composer(obj_id,pairs):
    url = f'https://graph.facebook.com/v9.0/{obj_id}?'
    url += '&'.join([x+'='+','.join(pairs[x]) for x in pairs.keys()])
    return url



for clave in dict_pages:
    print(clave)

    posts_info_df = None
    df_fb_comments = None

    current_id = dict_pages[clave]
    graph = facebook.GraphAPI(access_token=api_key, version = "3.1")
    token = graph.get_object(current_id, fields='access_token')

    months = [current_year1 + current_month1 + current_day1]
    for current_month in months:
        since =  datetime.strptime(f'{current_month[:4]}-{current_month[4:6]}-{current_month[6:]}T00:00:01','%Y-%m-%dT%H:%M:%S').timestamp()
        print(since)
        ny = int(current_month[:4])
        nm = int(current_month[4:6])
        nd = (int(current_month[6:]))+1
        if nd >= 31:
            nm += 1
            nd = 1
        if nm == 1:
            ny += 1
        print(ny, nm,nd)
        until =  datetime.strptime(f'{ny}-{nm}-{nd}T00:00:01','%Y-%m-%dT%H:%M:%S').timestamp()
        print(until)
#         print(current_month,since,until)
        response = requests.get(f'https://graph.facebook.com/v11.0/{current_id}?fields=access_token&access_token={api_key}')
        page_access_token = response.json()['access_token']
#         print(page_access_token)

        current_url= url_composer(current_id,
                                  #{'fields':[f'user.since({int(since)}).until({int(until)})'],
                                  {'fields':[f'published_posts.since({int(since)}).until({int(until)})','fan_count','followers_count'],
                                  #{'fields':[f'fan_count'],
                                   #'since':[since],
                                   #'until':[until],
                                   'access_token':[page_access_token]})

        #print(current_url)

        posts = []
        page = 0
        #print(current_url)
        try:
            response = requests.get(current_url).json()['published_posts']
            #print("RESPONSE", response)
        except:
            print('No encuentra post')
            continue

        #print(response.get('data')[0].get('created_time'))
        #print(response.get('published_posts'))
        #x = response.get('published_posts')
        #print(response['published_posts']['data'])

        while True:
            try:
                posts.extend(response['data'])
                if 'paging' in response.keys() and 'next' in response['paging'].keys():
                    current_url = response['paging']['next']
                    response = requests.get(current_url).json()
                else:
                    break
            except:
                pass
        print('Number of Posts',len(posts))
        posts_info = []
        cant = 0
        for post in posts:
            if cant%10 == 0:
                print(cant)
            cant += 1
            if 'story' in post.keys():
                continue
            curr_dict = post.copy()
            while True:
                current_url = url_composer(curr_dict['id']+'/insights',
                                           {'metric':['post_impressions',
                                                  'post_reactions_by_type_total',
                                                  ],
                                        'access_token':[page_access_token]})

                response = requests.get(current_url)
                if response.ok:
                    break
                print('ERROR')
                print(current_url)
                print(response.status_code)
                print(response.reason)
                #time.sleep(60*15)
            obj = response.json()
            for data_point in obj['data']:
                if type(data_point['values'][0]['value']) is dict:
                    for key in data_point['values'][0]['value'].keys():
                        curr_dict[data_point['name']+'_'+key] = data_point['values'][0]['value'][key]
                else:
                    curr_dict[data_point['name']] = data_point['values'][0]['value']
            while True:
                current_url = url_composer(curr_dict['id'],
                                   {'fields':['attachments',
                                              'shares',
                                              'comments.summary(true)',
                                              'likes.summary(true)',
                                              'message'],
                                    'access_token':[page_access_token]})
                response = requests.get(current_url)
                if response.ok:
                    break
                print('ERROR')
                print(current_url)
                print(response.status_code)
                print(response.reason)
                #time.sleep(60*15)
            obj = response.json()
            if 'attachments' in obj.keys():
                if 'title' in obj['attachments']['data'][0].keys():
                    curr_dict['attachment_title'] = obj['attachments']['data'][0]['title']
                if 'url' in obj['attachments']['data'][0].keys():
                    curr_dict['attachment_url'] = obj['attachments']['data'][0]['url']
            if 'shares' in obj.keys():
                curr_dict['share_count'] = obj['shares']['count']
            if 'comments' in obj.keys():
                curr_dict['comment_count'] = obj['comments']['summary']['total_count']
            posts_info.append(curr_dict)
        posts_info_df = pd.DataFrame(posts_info)

        # Crea base de datos
        #posts_info_df.to_csv(f's3://dataexport-sm-ctv/facebook_data/{current_month}/{current_page}.csv')

        #posts_info_df.to_csv("Publicaciones.csv")
        df_fb_comments = pd.DataFrame(columns=['id_publicacion','id_comment','message_post','message','created_time'])
        #print("Columns", posts_info_df.columns)
        if not posts_info_df.empty:
            for id_post,message in zip(posts_info_df['id'],posts_info_df['message']):  
                resultado = graph.get_object(id_post+"/comments",fields='created_time, message,id,comment_count,like_count' , access_token=token.get('access_token'))
                id_publicacion = id_post
                message_post = message
                comments = resultado['data']
                for datos in comments:
                    message = datos['message']
                    created_time = datos['created_time']
                    id_comment = datos['id']
                    comment_counts = datos['comment_count']
                    comment_likes = datos['like_count']
                    df_tmp = pd.DataFrame({'id_publicacion':id_publicacion,'id_comment':id_comment,'message_post':message_post,'message':message,'created_time':created_time,'comment_count':comment_counts,'comment_likes':comment_likes},index=[0])
                    df_fb_comments = df_fb_comments.append(df_tmp, ignore_index = True)
        else:
            print("NOT DATA FOUND")
    # Asigna nombre al archivo que se va a guardar (asignar nombre)
    #nombre = 'Publicaciones_Octubre'+clave+'.csv' ### 
    nombre = 'Publicaciones_'+clave+"_"+current_year1+current_month1+current_day1+".csv"

    #df_fb_comments.to_csv("prueba1_comentarios.csv")

    ##### AJUSTES NUEVOS SEGUIDORES - LIKES ##########
    current_url_followers = url_composer(current_id,
                                {'fields':[f'fan_count','followers_count'],
                                'access_token':[page_access_token]})

    try:
        r = requests.get(current_url_followers).json()
        #print("response", r)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)
        continue
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        continue
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        continue
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        continue

    fan_count = r['fan_count']
    followers_count = r['followers_count']
    df_fb_followers = pd.DataFrame({'fan_count':fan_count,'followers_count':followers_count},index=[0])

    followers = 'Followers_'+clave+"_"+current_year1+current_month1+current_day1+".csv"

    #df_fb_followers.to_csv("prueba_seguidore.csv")
    # Guarda en formato .csv
    #posts_info_df.to_csv('s3://rrss-caracol-narrativas/Marca/Facebook/Interno/'+clave+'/'+nombre )
    ######df_fb_comments.to_csv("prueba1_comentarios.csv")

    
    if posts_info_df is not None:
        print(posts_info_df.head())
        print('Primera parte')
        BUCKET = "rrss-caracol-ingestion-raw"
        posts_info_df.to_csv('/home/ubuntu/RRSS_CARACOL/facebook_data_analysis/files/post/'+nombre)
        s3 = boto3.resource("s3")
        s3.Bucket(BUCKET).upload_file('/home/ubuntu/RRSS_CARACOL/facebook_data_analysis/files/post/'+nombre,'Marca/Facebook/Interno/'+clave+'/'+nombre)

    if df_fb_comments is not None:
        comentarios = 'Comentarios_'+clave+"_"+current_year1+current_month1+current_day1+".csv"
        BUCKET = "rrss-caracol-ingestion-raw"
        df_fb_comments.to_csv('/home/ubuntu/RRSS_CARACOL/facebook_data_analysis/files/comentarios/'+comentarios)
        s3 = boto3.resource("s3")
        s3.Bucket(BUCKET).upload_file('/home/ubuntu/RRSS_CARACOL/facebook_data_analysis/files/comentarios/'+comentarios,'Marca/Facebook/Interno/'+clave+'/'+comentarios)
        
        # Asigna nombre al archivo que se va a guardar (asignar nombre)
        #nombre = 'Comentarios_Octubre.csv' ###  
        # Guarda en formato .csv
        #df_fb_comments.to_csv('s3://rrss-caracol-narrativas/Marca/Facebook/Interno/'+clave+'/'+nombre )      
        print('segunda parte')
        #df_fb_comments.to_csv("prueba1_comentarios.csv")
    

    if df_fb_followers is not None:
        print(df_fb_followers.head())
        BUCKET = "rrss-caracol-ingestion-raw"
        df_fb_followers.to_csv('/home/ubuntu/RRSS_CARACOL/facebook_data_analysis/files/followers/'+followers)
        s3 = boto3.resource("s3")
        s3.Bucket(BUCKET).upload_file('/home/ubuntu/RRSS_CARACOL/facebook_data_analysis/files/followers/'+followers,'Marca/Facebook/Interno/'+clave+'/'+followers)

