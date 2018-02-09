#!/bin/bash


 for i in `ls new_results`; 
 do
     #echo $i;
     findit=0
     for j in `ls composite_results_zip | grep $i `;
     do 
       #echo $j;
       findit=1
     done   
   
   
   if [[ $findit == 0 ]];then
     echo $i
     zip -r composite_results_zip/$i.zip new_results/$i
   fi      

 done
