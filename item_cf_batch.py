#!/usr/bin/env python
# coding: utf-8

# In[ ]:

print("START...")
import pandas as pd 
import numpy as np
import pymongo
from sklearn.metrics.pairwise import cosine_similarity
    
def main(): 
    
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
    itemuser_map=pd.DataFrame(useritem_map.transpose()).reset_index() 
    itemuser_map_nolabel=itemuser_map.drop(0).reset_index(drop=True)
    item_list=list(itemuser_map_nolabel['product_id'].values)
    
    def get_cslist(useritem_map): 
        cs=cosine_similarity(useritem_map)      # form an array of cosine similarity between users
        c_distance_full_list=cs.tolist()        # transform it into a nested list
        for i in range(len(c_distance_full_list)):
            for j in range(len(c_distance_full_list[i])):
                c_distance_full_list[i][j]=round(c_distance_full_list[i][j],4)
                if c_distance_full_list[i][j]==1.0:
                    c_distance_full_list[i][j]=-1.0        # adjustment 
        return c_distance_full_list
    
    def get_adjusted_cslist(itemuser_map):
        item_array=itemuser_map.values.astype(np.float)      
        means=item_array.mean(axis=1)
        item_mean=item_array-means[:,None]      # get normalised scores for each item 
        itemusermap_mean=pd.DataFrame(item_mean)
        frames=[itemuser_map.reset_index()['product_id'],itemusermap_mean]
        itemusermap_final=pd.concat(frames,axis=1,sort=False).set_index('product_id').round(4)      # get a normalised item user map 
        citem_distance_full_list=get_cslist(itemusermap_final)
        return itemusermap_final,citem_distance_full_list
    
    itemusermap_final,citem_distance_full_list=get_adjusted_cslist(itemuser_map_nolabel)
    def get_similar_items_forall(item_list,citem_distance_full_list):
        similars_list=[]
        for k in range(len(item_list)):
            target_list=citem_distance_full_list[k]
            similar_list=sorted(range(len(target_list)),key=lambda i:target_list[i],reverse=True)   
            transform_list=[]
            for j in range(len(similar_list)):
                transform_list.append(item_list[similar_list[j]])
            del transform_list[-1]
            similars_list.append(transform_list)
        slist=[]
        for i in range(len(similars_list)):
            itemdic={'product_id':item_list[i],'similar_products':similars_list[i]}
            slist.append(itemdic)
        return slist
    
    similar_item_list=get_similar_items_forall(item_list,citem_distance_full_list)
    
    def insert_into_mongoDB_forall(connecturl,DB,collection,item):     # function that inserts one or many records into mongoDB
        myclient=pymongo.MongoClient(connecturl)
        mydb=myclient[DB]
        mycol=mydb[collection]
        delete_from_mongo=mycol.delete_many({})
        sendall_to_mongo=mycol.insert_many(item)
        
    insert_into_mongoDB_forall("URL",'DBNAME','TABLENAME',similar_item_list)
    return 'Done'

if __name__=='__main__':
    main()

