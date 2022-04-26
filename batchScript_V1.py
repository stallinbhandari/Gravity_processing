# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 14:18:52 2022

@author: frenz
"""
#NOTE THIS SCRIPT IS APPLICABLE FOR LOOP SEQUENCE OBSERVATIONS ONLY
#THIS SCRIPT WORKS BY CREATING A SINGLE REDOBS FILE FOR ALL LOOPS
#THE LOOPS ARE SEPARATED IN THE FILE WITH A STANDARD SEPARATOR 

import pandas as pd
import os
import csv
#dat=pd.read_csv("D:\\Geodetic_Personal\\geodetic_field\\gravity\\Data_processing\\Data.csv",skiprows=20,delimiter='\t')

def make_job(nam,st,g,sd):
    with open ('loops/'+nam+'.job','w') as f1:
        f1.write ('gradj <<!\n'+'loops\\'+nam+'.redobs'+'\n'+'loops\\'+nam+'.ogradj'+'\n'+'loops\\'+nam+'.resgradj'+'\n'+'loops\\'+nam+'.tiegradj'+'\n'+'24 f 0.5'+'\n'+'1'+'\n'+str(st)+' '+str(g)+' '+str(sd)+'\n'+'!')
    
    

dat=pd.read_csv("CG-6_0158_GRAV7879.dat", skiprows=20,delimiter='\t')
dat=dat[dat['StdDev']!="******"]
#dates=dat.Date.unique()
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
        
raw_grav=dat.RawGrav.to_list()
cor_grav=dat.CorrGrav.to_list()
st_name=dat['/Station']
corr=dat['TideCorr']+dat['TiltCorr']+dat['TempCorr']+dat['DriftCorr']
coord={str(st_code[i]):[lat[i],lon[i],ht[i]] for i in range (len(st_code))}
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
separators=[]
with open ('grav7879.redobs','w') as f1:
    for x in indices:
        
        start=indices[x][0] #values for key x from indices dictionary
        stat=redobs_dat.iloc[start][0] #station code of the startng station
        end=indices[x][len(indices[x])-1]
        inds=[i for i in indices[x] if i > prev_index]
        #print (inds)
        prev_index=end
        
        if inds !=[]:
            dat_select=pd.DataFrame()
            dat_select=redobs_dat.iloc[range(inds[0],inds[-1]+1)]
            dat_select.dropna()
            #print ('loop starts at station',stat)
            #dat_select.to_csv('loops\\loop'+str(count)+'.redobs', sep='\t',header=None,index=False)
            
            separator='# G-000000019030158'+'  '+str(dat_select['date'].unique()[0])
            print (separator)
            print ('============================')
            
            print (dat_select)
            dat_select.to_csv('grav7879.redobs',header=None,sep='\t',mode='a',index=False)
            
            for i in range(0,len(dat_select)):
                i=i-1
                if i<0:
                    f1.write(separator+'\n')
                    separators.append(separator)
                else:
                    f1.write("   ".join(str(i) for i in dat_select.iloc[i+1].tolist())+'\n')
            #nam='loop'+str(count)
            
            st=dat_select.iloc[0]['St_code']
            #print(count)
            #count+=1
            
           #making file for adjustment and executing it 
    #=============================================================================
    
            #print (dat_select)
                                
nam='grav7879'
st=1
g,sd=gravities['1'][0], gravities['1'][1]

with open ('grav7879.inp','w') as f1:
    f1.write (nam+'.redobs'+'\n'+nam+'.ogradj'+'\n'+nam+'.resgradj'+'\n'+nam+'.tiegradj'+'\n'+'24 f 0.5'+'\n'+'1'+'\n'+str(st)+' '+str(g)+' '+str(sd)+'\n'+'# S-000000019030158'+'\n')
    
os.system('gradj <grav7879.inp >grav7879.out')   

with open(nam+'.ogradj','r') as file3:
    f3=file3.readlines()
    gravs_start=f3.index(' #== Adjusted new gravity values and standard deviations ===\n')
    gravs_end=f3.index(' === Statistics of adjustment ===\n')
    search=f3[gravs_start+1:gravs_end]
    for item in search:
        b=item.strip().split()
        if len(b)!=0:
            print ('items',item)
            if str(b[0]) not in gravities:
                gravities [b[0]]=[b[1],b[2]]
        
#=============================================================================
                          
    
    #print (start,end)
   
with open ('gravities_V1.txt','w') as file4:
    for keys in name_code and gravities:
        print (keys,name_code[keys],gravities[keys][0],gravities[keys][1])
    #for keys in gravities:
        writ=[keys,name_code[keys],coord[keys][0],coord[keys][1],coord[keys][2],gravities[keys][0],gravities[keys][1]]
        #print (writ)
        file4.write("\t".join (str(i) for i in writ)+'\n')      
        

      
    
        
    
    
    
   

    
    
    

            
    