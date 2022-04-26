# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 14:18:52 2022

@author: Stallin Bhandari
"""

#NOTE THIS SCRIPT IS APPLICABLE FOR LOOP SEQUENCE OBSERVATIONS ONLY
#THIS SCRIPT WORKS BY CREATING JOB FILES FOR EACH LOOP AND EXECUTING JOB FILES

import pandas as pd
import os
#dat=pd.read_csv("D:\\Geodetic_Personal\\geodetic_field\\gravity\\Data_processing\\Data.csv",skiprows=20,delimiter='\t')

def make_job(nam,st,g,sd):
    with open ('loops/'+nam+'.job','w') as f1:
        f1.write ('gradj <<!\n'+'loops\\'+nam+'.redobs'+'\n'+'loops\\'+nam+'.ogradj'+'\n'+'loops\\'+nam+'.resgradj'+'\n'+'loops\\'+nam+'.tiegradj'+'\n'+'24 f 0.5'+'\n'+'1'+'\n'+str(st)+' '+str(g)+' '+str(sd)+'\n'+'!')
    
    

dat=pd.read_csv("CG-6_0158_GRAV7879.dat", skiprows=20,delimiter='\t')
#dates=dat.Date.unique()
dat=dat[dat['StdDev']!="******"]
i=1
stations= dat['/Station'].unique()
st_code= [i for i in range (1,len(stations)+1)]
dates=dat.Date.tolist()
times=dat.Time.tolist()
date_ro=[]
time_ro=[]
serial=[x for x in range (1,len(dat)+1)]
for s in dates:
    #date_ro.append(s[8:]+s[5:7]+s[2:4])
    date_ro.append(s[8:]+s[5:7]+s[2:4])
for t in times:
    time_ro.append(t[0:2]+'.'+t[3:5])
    
    
lat=[]
lon=[]
ht=[]
for i in stations:
    b=dat[dat['/Station']==i]
    lat1=b['LatUser'].max()
    lat.append(lat1)
    lon1=b['LonUser'].max()
    lon.append(lon1)
    ht1=b['ElevUser'].max()
    ht.append(ht1)
    print(b)


coord={str(st_code[i]):[lat[i],lon[i],ht[i]] for i in range (len(st_code))}    
raw_grav=dat.RawGrav.to_list()
cor_grav=dat.CorrGrav.to_list()
st_name=dat['/Station']
corr=dat['TideCorr']+dat['TiltCorr']+dat['TempCorr']+dat['DriftCorr']

st_no=[]
name_code={str(st_code[i]):stations[i] for i in range (len(stations))}

for x in dat['/Station']:
    for key in name_code:
        if x==name_code[key]:
            st_no.append(key)
#dat1=pd.DataFrame()
#dat1['St_code','date','time','sno','rawgrav','corr','corgrav','stname']=st_no, date_ro, time_ro,serial,raw_grav,cor_grav,st_name       
# =============================================================================
# for a,b,c,d,e,f,g,h in zip (st_no, date_ro, time_ro,serial,raw_grav,corr,cor_grav,st_name):
#     print (a,b,c,d,e,f,g,h)
# =============================================================================
col_names=['St_code','date','time','sno','rawgrav','corr','corgrav','stname']        
list_dat=list(zip(st_no, date_ro, time_ro,serial,raw_grav,corr,cor_grav,st_name))  
redobs_dat=pd.DataFrame(list_dat,columns=col_names)
#Determining loops
#Assuming that a loop doesnot span across days i.e. for us there wont be observations after 6:15
#Assuming loop closes at the same station
count=1
j=0
# =============================================================================
# for i in range (len(redobs_dat)):
#     print (redobs_dat.iloc[i].values )       
# =============================================================================
             
#finding indices of each station:
indices={}
for st in stations:
    ind=redobs_dat.index[redobs_dat['stname']==st].tolist()
    indices[st]=ind
    
redobs_dat.to_csv('redobs_all.redobs',sep='\t',index=False,header=None)

#Selecting loops
prev_index=-1
count=1
#gravity of initial base
gravities={'1':['978663.202','0.014']} #Absolute gravity value for KATHAGB- station
loop_starter={'1':['978663.202','0.014']}
for x in indices:
    
    start=indices[x][0]
    stat=redobs_dat.iloc[start][0]
    end=indices[x][len(indices[x])-1]
    inds=[i for i in indices[x] if i > prev_index]
    #print (inds)
    prev_index=end
    dat_select=pd.DataFrame()
    if inds !=[]:
        dat_select=redobs_dat.iloc[range(inds[0],inds[-1]+1)]
        dat_select.dropna()
        print ('loop starts at station',stat)
        dat_select.to_csv('loops\\loop'+str(count)+'.redobs', sep='\t',header=None,index=False)
        
        nam='loop'+str(count)
        st=dat_select.iloc[0]['St_code']
        #print(count)
        #count+=1
        
       #making file for adjustment and executing it 
#=============================================================================
        
        if count==1:            
            grav=gravities[str(redobs_dat.iloc[start]['St_code'])][0]
            sd=gravities[str(redobs_dat.iloc[start]['St_code'])][1]
            make_job(nam,st,grav,sd)
            os.system('job loops/'+nam)
            print (nam,'count',count,grav,sd)
            count+=1
        elif count>1:
            prev_file='loop'+str(count-1)
            print ('gravity value from {} and new file {}'.format(prev_file,nam))
            with open ('loops/'+prev_file+'.ogradj','r') as file2:
                print ('loops/'+prev_file+'.ogradj file opened')
                f2=file2.readlines()
                #print (f2)
                gravs_start=f2.index(' #== Adjusted new gravity values and standard deviations ===\n')
                gravs_end=f2.index(' === Statistics of adjustment ===\n')
                search=f2[gravs_start+1:gravs_end]
                print ('search:\n',search)
                for item in search:
                    b=item.strip().split()
                    if len(b)!=0  :
                        print ('item=',item,b[0])
                        if str(b[0]) not in gravities:
                            gravities[b[0]]=[b[1],b[2]]
                        
                        #print (stat)
                        if str(stat) in b:
                            #st=b[0]
                            print ('previous gravity station and values printed:',b)
                            grav=b[1]
                            sd=b[2]
                            print('Job file made for {} with:'.format(nam))
                            make_job(nam,stat,grav,sd)
                            print ('known station {},gravity value{},standard deviation{}'.format(st,grav,sd))
                            
                            loop_starter[str(st)]=[grav,sd]
                            print ('job file run for {}'.format(nam))
                            os.system('job loops\\'+nam)
                            count+=1
                            print('=================================================')
            
        #print (dat_select)
                            
with open('loops/'+nam+'.ogradj','r') as file3:
    f3=file3.readlines()
    gravs_start=f2.index(' #== Adjusted new gravity values and standard deviations ===\n')
    gravs_end=f2.index(' === Statistics of adjustment ===\n')
    search=f3[gravs_start+1:gravs_end]
    for item in search:
        b=item.strip().split()
        if len(b)!=0:
            print ('items',item)
            if str(b[0]) not in gravities:
                gravities [b[0]]=[b[1],b[2]]
        
#=============================================================================
                          
    
    #print (start,end)
   
with open ('gravities_V0.txt','w') as file4:
    for keys in name_code and gravities:
        print (keys,name_code[keys],gravities[keys][0],gravities[keys][1])
    #for keys in gravities:
        writ=[keys,name_code[keys],coord[keys][0],coord[keys][1],coord[keys][2],gravities[keys][0],gravities[keys][1]]
        #print (writ)
        file4.write("\t".join (str(i) for i in writ)+'\n')      
        
        

      
    
        
    
    
    
   

    
    
    

            
    