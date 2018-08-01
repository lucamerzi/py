
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


get_ipython().run_line_magic('pwd', '')


# In[4]:


df = pd.read_csv("C:\Users\lmerzetti\Google Drive\Luca\clusters_keywords\keywords-final.csv")
df.head()


# In[10]:


df_final = df[(df["lang"] != "ru") & (df["lang"] != "zh-cn")]


# In[13]:


df_final.head()


# In[14]:


df_final.shape


# In[15]:


df_final.to_csv("cleaned-lang.csv", index=False)


# In[16]:


get_ipython().run_line_magic('pwd', '')


# In[28]:


df_filtered = df[(df["lang"] == "fr") | (df["lang"] == "en")]


# In[29]:


#tester methode .query()
#df.query('a == 4 & b != 2')


# In[30]:


df_filtered.shape

