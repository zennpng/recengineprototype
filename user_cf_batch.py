#!/usr/bin/env python
# coding: utf-8

# In[ ]:

print("START...")
import pandas as pd 
import pymongo
from sklearn.metrics.pairwise import cosine_similarity
    
def main(similarity_no): 
    
    def fetch_from_MongoDB(server,DB,collection):     
        myclient=pymongo.MongoClient(server)
        db=myclient[DB]
        collection=db[collection]
        return pd.DataFrame(list(collection.find()))   
    
    df=fetch_from_MongoDB("URL",'DBNAME','TABLENAME')
    
    def get_useritem_map(df):                                                                     
        df1=df['user_id']
        df2=df['product_id']
        df3=df['score']
        frame=[df1,df2,df3]
        modified_map=pd.concat(frame,axis=1)
        useritem_map=modified_map.pivot(index='user_id', columns='product_id', values='score').fillna(0).reset_index()    
        return useritem_map

    useritem_map=get_useritem_map(df)
    
    def get_user_info(useritem_map):
        user_list = useritem_map['user_id'].tolist() 
        return user_list

    user_list=get_user_info(useritem_map)
    
    def get_cslist(useritem_map): 
        cs=cosine_similarity(useritem_map)      # form an array of cosine similarity between users
        c_distance_full_list=cs.tolist()        # transform it into a nested list
        for i in range(len(c_distance_full_list)):
            for j in range(len(c_distance_full_list[i])):
                c_distance_full_list[i][j]=round(c_distance_full_list[i][j],4)
                if c_distance_full_list[i][j]==1.0:
                    c_distance_full_list[i][j]=0.0
        return c_distance_full_list
    
    c_distance_full_list=get_cslist(useritem_map)     # get cosine distance list
    
    def get_similar_users_forall(user_list,c_distance_full_list):
        similars_list=[]
        for k in range(len(user_list)):
            target_list=c_distance_full_list[k]
            similar_list=sorted(range(len(target_list)),key=lambda i:target_list[i],reverse=True)[0:similarity_no]     # top 3 similar users selected
            transform_list=[]
            for j in range(len(similar_list)):
                transform_list.append(user_list[similar_list[j]])
            similars_list.append(transform_list)
        slist=[]
        for i in range(len(similars_list)):
            userdic={'user_id':user_list[i],'similar_users':similars_list[i]}
            slist.append(userdic)
        return slist
    
    similar_user_list=get_similar_users_forall(user_list,c_distance_full_list)    # get indexes of target user and similar users
    
    def insert_into_mongoDB_forall(connecturl,DB,collection,item):     # function that inserts one or many records into mongoDB
        myclient=pymongo.MongoClient(connecturl)
        mydb=myclient[DB]
        mycol=mydb[collection]
        delete_from_mongo=mycol.delete_many({})
        sendall_to_mongo=mycol.insert_many(item)
        
    insert_into_mongoDB_forall("URL",'DBNAME','TABLENAME',similar_user_list)
    return 'Done'
    
if __name__=='__main__':
    main(3)   # set neighbour range to 3
