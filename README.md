## Recommendation Engine Service Prototype (Compute Side)

language: Python 3.7  
requirements: pip install -r requirements.txt  
database: MongoDB Atlas (admin access + ip whitelist)  
run: Windows Task Scheduler (periodic compute)  

- Change path within all batch files 
- Configure server string in python files with valid credentials 
- Configure neural network's hyperparameters in deep_content_filtering_batch.py

Filtering Logics: 
- content filtering (item-item)
- content filtering (learning)
- collaborative filtering (user-item)
- collaborative filtering (user-user)
    
