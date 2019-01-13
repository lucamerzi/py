# arr = [1, 2, 3]
# arr_copy = arr.copy()
# print(arr_copy)
# print(arr == arr_copy)


planet = "Earth"
diameter = 12742
print("The diameter of {x} is {y} kilometers.".format(y=diameter, x=planet))


# Given this nested list, use indexing to grab the word "hello" 
lst = [1,2,[3,4],[5,[100,200,['hello']],23,11],1,7]
print(lst[3][1][2][0])


# Given this nested dictionary grab the word "hello". Be prepared, this will be annoying/tricky 
d = {'k1':[1,2,3,{'tricky':['oh','man','inception',{'target':[1,2,3,'hello']}]}]}
print(d["k1"][3]["tricky"][3]["target"][3])


# Create a function that grabs the email website domain from a string in the form: 
# user@domain.com
# So for example, passing "user@domain.com" would return: domain.com
def mail_extractor(mail):
  return mail.split("@")[1]

print(mail_extractor("user@domain.com"))


#Create a basic function that returns True if the word 'dog' is contained in the input string. Don't worry about edge cases like a punctuation being attached to the word dog, but do account for capitalization.
def findDog(phr):
  return len(phr.lower().split("dog")) > 1

print (findDog("Is there a Dog here"))


# Create a function that counts the number of times the word "dog" occurs in a string. Again ignore edge cases
def countDog(phr):
  return len(phr.lower().split("dog")) - 1

print(countDog('This dog runs faster than the other dog dog dog dude!'))


# Use lambda expressions and the filter() function to filter out words from a list that don't start with the letter 's'. For example:
seq = ['soup','dog','salad','cat','great']
# should be filtered down to:
['soup','salad']

filtered = list(filter((lambda x : x[0] == "s"), seq))
print(filtered)


# Final Problem
# You are driving a little too fast, and a police officer stops you. Write a function to return one of 3 possible results: "No ticket", "Small ticket", or "Big Ticket". If your speed is 60 or less, the result is "No Ticket". If speed is between 61 and 80 inclusive, the result is "Small Ticket". If speed is 81 or more, the result is "Big Ticket". Unless it is your birthday (encoded as a boolean value in the parameters of the function) -- on your birthday, your speed can be 5 higher in all cases.

def caught_speeding(speed, is_birthday):
  
  if is_birthday: 
    speed -= 5
  
  if (speed <= 60):
    return "No ticket"
  elif (speed > 60 and speed <= 80):
    return "Small ticket"
  else:
    return "Big ticket"

print(caught_speeding(81, True))  # Small ticket
print(caught_speeding(81, False)) # Big ticket