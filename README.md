# home-page-analytics-feed
Sample Script to update an article with results from analytics queries

##Prerequisites
1. Create a Rich Text Custom Field
2. Create a Custom Article Template called "Homepage Feed"
3. Create an article with the new template
4. Configure all the parameters in the config_feed.ini file


## Parameters
Add the following parameters to the config_feed.ini file:

[api]\
alation_base_url = https://diproject.alationcloud.com\ 
refresh_token = HyWltN79F-ALuzZ_iKT3fJTmFVe8nEjFPPbn-khFVy41kSKhSkDuyh989hpItGAqi79-Jjrko68mMchChxUWmg \
user_id = 4\ 

[article]
template_id = 0 \  
article_template = Homepage Feed \ 
custom_article = What's happening in Alation? \ 


