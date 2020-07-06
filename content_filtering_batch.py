#!/usr/bin/env python
# coding: utf-8

# In[ ]:

print("START...")
import pandas as pd 
import pymongo
    
def main(): 
    
    def fetch_from_MongoDB(server,DB,collection):     
        myclient=pymongo.MongoClient(server)
        db=myclient[DB]
        collection=db[collection]
        return pd.DataFrame(list(collection.find()))   
    
    df=fetch_from_MongoDB("URL",'DBNAME','TABLENAME')
    
    def get_similar_items():
        attributes_list=df['attributes'].tolist()
        item_list=df['product_id'].tolist()
        similar_items_list=[]
        similar_item_attributes_list=[]
        for i in range(len(attributes_list)):
            similar_items_list1=[]
            similar_item_attributes_list1=[]
            for j in range(len(attributes_list)):
                if j == i:
                    pass
                else:
                    if attributes_list[j]['product_category'] == attributes_list[i]['product_category'] and attributes_list[j]['product_subcategory'] == attributes_list[i]['product_subcategory']:
                        attributes_list[j]['product_department']=abs(attributes_list[j]['product_department']-attributes_list[i]['product_department'])
                        attributes_list[j]['product_price']=abs(attributes_list[j]['product_price']-attributes_list[i]['product_price'])
                        attributes_list[j]['product_colour']=abs(attributes_list[j]['product_colour']-attributes_list[i]['product_colour'])
                        attributes_list[j]['product_brand']=abs(attributes_list[j]['product_brand']-attributes_list[i]['product_brand'])
                        attributes_list[j]['total_difference']=(attributes_list[j]['product_department']
                                                                +(attributes_list[j]['product_price']/5)
                                                                +attributes_list[j]['product_colour']
                                                                +attributes_list[j]['product_brand'])
                        similar_item_attributes_list1.append(attributes_list[j])
                        similar_items_list1.append(item_list[j])
            similar_items_list.append(similar_items_list1)
            similar_item_attributes_list.append(similar_item_attributes_list1)
        return item_list,similar_items_list,similar_item_attributes_list
    
    item_list,similar_items_list,similar_item_attributes_list=get_similar_items() 
    
    final_items_list=[]
    for i in range(len(similar_item_attributes_list)):
        z=dict(zip(similar_items_list[i],similar_item_attributes_list[i]))
        final_items_list.append(sorted(z, key=lambda k: z[k]['total_difference']))
    outlist=[]
    for i in range(len(final_items_list)):
            itemdic={'product_id':item_list[i],'similar_products':final_items_list[i]}
            outlist.append(itemdic)
            
    def insert_into_mongoDB_forall(connecturl,DB,collection,item):     # function that inserts one or many records into mongoDB
        myclient=pymongo.MongoClient(connecturl)
        mydb=myclient[DB]
        mycol=mydb[collection]
        delete_from_mongo=mycol.delete_many({})
        sendall_to_mongo=mycol.insert_many(item)
        
    insert_into_mongoDB_forall("URL",'DBNAME','TABLENAME',outlist)        
    return 'Done' 

if __name__=='__main__':
    main()

