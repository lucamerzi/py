
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


#print working directory
get_ipython().run_line_magic('pwd', '')


# In[4]:


#read csv file
df = pd.read_csv("/Users/lucamerzetti/Google Drive/Luca/clusters_keywords/out_global.csv")
df


# In[5]:


type(df)


# In[7]:


#save dataframe to csv without index
df.to_csv("keywords-cleaned.csv", index=False);


# In[8]:


df.shape


# In[14]:


#get column names
col_names = df.columns.tolist()
print(col_names)


# In[16]:


#rename column
df.columns = ['a','b','c']
df.head()


# In[21]:


#'WHERE' condition is met on column a
df[df["a"] != "Cycling"] 


# In[25]:


#'WHERE' condition is met on column a
df[(df["a"] != "Cycling") & (df["b"] == "zh-cn")]


# In[26]:


df[(df["a"] != "Cycling") & (df["b"] == "zh-cn")].shape


# In[29]:


df["value"] = df.apply(lambda x: 1, axis = 1)


# In[30]:


df.head()


# In[31]:


df.tail()


# In[33]:


df.columns = ["cluster","lang","keyword","default"]
df.to_csv("keywords-final.csv", index=False)

