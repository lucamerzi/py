#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 15:22:43 2017

@author: antoinekrainc
"""

# Average spendings on Adwords

import numpy as np
import pandas as pd
import math  
    # Import dataset
dataset = pd.read_excel("Monthly spending on adwords.xlsx")

    # Calculate Mean & Standard Dev
mean = dataset["Monthly spending"].mean()
stdev = dataset["Monthly spending"].std()

    # Set the confidence interval
confidence = 95/100
    
    # Set the corresponding t Value 
t = 2.04

    # Find the interval 

real_mean = [mean + t*stdev/math.sqrt(31), mean - t*stdev/math.sqrt(31)]

