""" Create queries to ask the following three questions of the data """

""" 1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4 """

# Create a for loop to create a list of files and collect each filepath
# join the file path and roots with the subdirectories using glob
for root, dirs, files in os.walk(filepath):
    file_path_list = glob.glob(os.path.join(root,'*'))

#####################################
print("debug --> to see file_path_list ....\n")
print(file_path_list)
  
print("debug --> to see len(file_path_list) ...\n")
print(len(file_path_list))