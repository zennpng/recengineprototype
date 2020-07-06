#!/usr/bin/env python
# coding: utf-8

# In[ ]:

print("START...")
import pymongo
import pandas as pd
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
import numpy as np
from joblib import dump, load
from pathlib import Path

def main():
    
    def fetch_from_MongoDB(server,DB,collection) :     
            myclient=pymongo.MongoClient(server)
            db=myclient[DB]
            collection=db[collection]
            return pd.DataFrame(list(collection.find()))   
    df=fetch_from_MongoDB("URL",'DBNAME','TABLENAME')
    item_collection=fetch_from_MongoDB("URL",'DBNAME','TABLENAME')

    def get_useritem_map(df):                                                                     
        df1=df['user_id']
        df2=df['product_id']
        df3=df['score']
        frame=[df1,df2,df3]
        modified_map=pd.concat(frame,axis=1)
        useritem_map=modified_map.pivot(index='user_id', columns='product_id', values='score').fillna(0).reset_index()    
        return useritem_map
    useritem_map=get_useritem_map(df)
    transposed_map=useritem_map.transpose().drop(['user_id'])

    slist=[]
    for i in range(len(useritem_map['user_id'])):
        user_id = useritem_map['user_id'][i]
        items_notsearched=transposed_map.index[transposed_map[i]==0].tolist()
        items_searched=transposed_map.index[transposed_map[i]!=0].tolist()
        items_searched_scores=[]
        for j in range(len(transposed_map[i])):
            if transposed_map[i][j]!=0:
                 items_searched_scores.append(transposed_map[i][j])
        items_searched_attributes=[]
        items_notsearched_attributes=[]
        for i in range(len(item_collection['product_id'])):
            if item_collection['product_id'][i] in items_searched:
                attribute_seen=list(item_collection['attributes'][i].values())
                items_searched_attributes.append(attribute_seen)
            else:
                attribute_notseen=list(item_collection['attributes'][i].values())
                items_notsearched_attributes.append(attribute_notseen)
        Y_train=np.float_(items_searched_scores)
        X_train=np.float_(items_searched_attributes)
        X_test=np.float_(items_notsearched_attributes)
        scaler = StandardScaler()
        scaler.fit(X_train)
        X_train = scaler.transform(X_train)
        X_test = scaler.transform(X_test)
        mlp = MLPRegressor(hidden_layer_sizes=(30,30),activation='identity',solver='adam',learning_rate='adaptive',max_iter=200,learning_rate_init=0.001) 
        target_file=Path("Joblib trained models/{}.joblib".format(user_id))
        if target_file.is_file():
            mlp=load('Joblib trained models/{}.joblib'.format(user_id))
        else:
            pass
        mlp.fit(X_train, Y_train)    
        dump(mlp, 'Joblib trained models/{}.joblib'.format(user_id))     # store the model in a joblib file with the following path
        predictions=list(mlp.predict(X_test)) 
        predicted_classes=[round(i,2) for i in predictions]
        highest_scoreclass=predicted_classes.copy()
        sorted_scoreclass,sorted_items=list(zip(*sorted(zip(highest_scoreclass,items_notsearched,),reverse=True)))
        recdic={'user_id':user_id,'similar_products':sorted_items}
        slist.append(recdic)

    def insert_into_mongoDB_forall(connecturl,DB,collection,item):
        myclient=pymongo.MongoClient(connecturl)
        mydb=myclient[DB]
        mycol=mydb[collection]
        delete_from_mongo=mycol.delete_many({})
        sendall_to_mongo=mycol.insert_many(item)
    insert_into_mongoDB_forall("URL",'DBNAME','TABLENAME',slist)
    return "Done"

if __name__=='__main__':
    main()

