**TV Viewership Analysis using Spark**  
  
This project will have you perform Data Analysis and processing using Apache Spark.   
**Dataset Source:** http://recsys.deib.polimi.it/?page_id=76   
The above dataset contains the data relative to tv watching behaviour for 19 weeks. Since the data from the week 14 has some errors, we are going to clean this data and only use the data for the first 13 weeks for this analysis. The metadata for the used dataset is as follows.   

**Columns are:.**  
channel ID: channel id from 1 to 217.  
slot: hour inside the week relative to the start of the view, from 1 to 24*7 = 168.  
week: week from 1 to 19. Weeks 14 and 19 should not be used because they contain errors.  
genre ID: it is the id of the genre, form 1 to 8. Genre/subgenre mapping is in genres_legend.txt file.  
subGenre ID: it is the id of the subgenre, from 1 to 114. Genre/subgenre mapping is in genres_legend.txt file.  
user ID: it is the id of the user.  
program ID: it is the id of the program. The same program can occur multiple times (e.g. a tv show).  
event ID: it is the id of the particular instance of a program. It is unique, but it can span multiple slots.  
duration: duration of the view.   
  
Fields are delimited with a comma.     
      
**In particular, this project yields the analysis of following insights:**    
  
**1. Timeframe with maximum number of users watching television.**    
Useful for advertisers to show their ads to reach maximum audience.  
  
**2. Best slot in a week to host a new show.**  
Useful for tv channels to plan their show times for any new show.  
  
**3. Best channel for a particular time in a day.**  
Advertisers can get to know the best channel for any particular time in a day to display their ads.  
  
**4. Best genre .**   
To know what genres viewers are interested in.  

**5. Best sub genres for any partuclar genre.**  
To know which sub genres are more watched for any particular genre and come up with new shows that interests it's viewers.  
  

