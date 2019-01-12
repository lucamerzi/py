#!/usr/bin/env python
# coding: utf-8

# ___
# 
# <a href='http://www.pieriandata.com'> <img src='../Pierian_Data_Logo.png' /></a>
# ___
# # Python Crash Course
# This notebook will just go through the basic topics in order:
# 
# * Data types
#     * Numbers
#     * Strings
#     * Printing
#     * Lists
#     * Dictionaries
#     * Booleans
#     * Tuples 
#     * Sets
# * Comparison Operators
# * if, elif, else Statements
# * for Loops
# * while Loops
# * range()
# * list comprehension
# * functions
# * lambda expressions
# * map and filter
# * methods
# ____

# ## Data types
# 
# ### Numbers

# In[1]:


1 + 1


# In[2]:


1 * 3


# In[3]:


1 / 2


# In[4]:


2 ** 4


# In[5]:


4 % 2


# In[6]:


5 % 2


# In[7]:


(2 + 3) * (5 + 5)


# ### Variable Assignment

# In[13]:


# Can not start with number or special characters
name_of_var = 2


# In[8]:


x = 2
y = 3


# In[9]:


z = x + y


# In[10]:


z


# ### Strings

# In[11]:


'single quotes'


# In[12]:


"double quotes"


# In[13]:


" wrap lot's of other quotes"


# ### Printing

# In[14]:


x = 'hello'


# In[15]:


x


# In[16]:


print(x)


# In[17]:


num = 12
name = 'Sam'


# In[24]:


print('My number is: {one}, and my name is: {two}'.format(one=num,two=name))


# In[25]:


print('My number is: {}, and my name is: {}'.format(num,name))


# ### Lists

# In[26]:


[1,2,3]


# In[27]:


['hi',1,[1,2]]


# In[28]:


my_list = ['a','b','c']


# In[29]:


my_list.append('d')


# In[30]:


my_list


# In[31]:


my_list[0]


# In[32]:


my_list[1]


# In[33]:


my_list[1:]


# In[34]:


my_list[:1]


# In[35]:


my_list[0] = 'NEW'


# In[98]:


my_list


# In[99]:


nest = [1,2,3,[4,5,['target']]]


# In[100]:


nest[3]


# In[101]:


nest[3][2]


# In[102]:


nest[3][2][0]


# ### Dictionaries

# In[37]:


d = {'key1':'item1','key2':'item2'}


# In[38]:


d


# In[39]:


d['key1']


# ### Booleans

# In[40]:


True


# In[41]:


False


# ### Tuples 

# In[42]:


t = (1,2,3)


# In[43]:


t[0]


# In[44]:


t[0] = 'NEW'


# ### Sets

# In[45]:


{1,2,3}


# In[46]:


{1,2,3,1,2,1,2,3,3,3,3,2,2,2,1,1,2}


# ## Comparison Operators

# In[47]:


1 > 2


# In[48]:


1 < 2


# In[49]:


1 >= 1


# In[50]:


1 <= 4


# In[51]:


1 == 1


# In[52]:


'hi' == 'bye'


# ## Logic Operators

# In[53]:


(1 > 2) and (2 < 3)


# In[54]:


(1 > 2) or (2 < 3)


# In[55]:


(1 == 2) or (2 == 3) or (4 == 4)


# ## if,elif, else Statements

# In[56]:


if 1 < 2:
    print('Yep!')


# In[57]:


if 1 < 2:
    print('yep!')


# In[58]:


if 1 < 2:
    print('first')
else:
    print('last')


# In[59]:


if 1 > 2:
    print('first')
else:
    print('last')


# In[60]:


if 1 == 2:
    print('first')
elif 3 == 3:
    print('middle')
else:
    print('Last')


# ## for Loops

# In[61]:


seq = [1,2,3,4,5]


# In[62]:


for item in seq:
    print(item)


# In[63]:


for item in seq:
    print('Yep')


# In[64]:


for jelly in seq:
    print(jelly+jelly)


# ## while Loops

# In[65]:


i = 1
while i < 5:
    print('i is: {}'.format(i))
    i = i+1


# ## range()

# In[66]:


range(5)


# In[67]:


for i in range(5):
    print(i)


# In[68]:


list(range(5))


# ## list comprehension

# In[69]:


x = [1,2,3,4]


# In[70]:


out = []
for item in x:
    out.append(item**2)
print(out)


# In[71]:


[item**2 for item in x]


# ## functions

# In[72]:


def my_func(param1='default'):
    """
    Docstring goes here.
    """
    print(param1)


# In[73]:


my_func


# In[74]:


my_func()


# In[75]:


my_func('new param')


# In[76]:


my_func(param1='new param')


# In[77]:


def square(x):
    return x**2


# In[78]:


out = square(2)


# In[79]:


print(out)


# ## lambda expressions

# In[80]:


def times2(var):
    return var*2


# In[81]:


times2(2)


# In[82]:


lambda var: var*2


# ## map and filter

# In[83]:


seq = [1,2,3,4,5]


# In[84]:


map(times2,seq)


# In[85]:


list(map(times2,seq))


# In[86]:


list(map(lambda var: var*2,seq))


# In[87]:


filter(lambda item: item%2 == 0,seq)


# In[88]:





# ## methods

# In[111]:


st = 'hello my name is Sam'


# In[112]:


st.lower()


# In[113]:


st.upper()


# In[103]:


st.split()


# In[104]:


tweet = 'Go Sports! #Sports'


# In[106]:


tweet.split('#')


# In[107]:


tweet.split('#')[1]


# In[92]:


d


# In[93]:


d.keys()


# In[94]:


d.items()


# In[95]:


lst = [1,2,3]


# In[96]:


lst.pop()


# In[108]:


lst


# In[109]:


'x' in [1,2,3]


# In[110]:


'x' in ['x','y','z']


# # Great Job!
